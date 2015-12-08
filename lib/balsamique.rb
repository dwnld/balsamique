require 'digest/sha1'
require 'json'
require 'redis'

class Balsamique
  def initialize(redis, namespace = 'bQ')
    @redis = redis

    @que_prefix = namespace + ':que:'
    @questats_prefix = namespace + ':questats:'
    @env_prefix = namespace + ':env:'

    @status = namespace + ':status'
    @queues = namespace + ':queues'
    @retries = namespace + ':retries'
    @failures = namespace + ':failures'
    @failz = namespace + ':failz'
    @unique = namespace + ':unique'
    @tasks = namespace + ':tasks'
    @args = namespace + ':args'
    @report_queue = @que_prefix + '_report'
  end

  REPORT_RETRY_DELAY = 60.0 # seconds
  RETRY_DELAY = 600.0 # seconds

  def redis
    @redis
  end

  def redis_eval(cmd_sha, cmd, keys, argv)
    redis.evalsha(cmd_sha, keys, argv)
  rescue Redis::CommandError
    puts "[INFO] Balsamique falling back to EVAL for #{cmd_sha}"
    redis.eval(cmd, keys, argv)
  end

  def self.next_task(tasks)
    item = tasks.find { |t| t.size == 1 }
    item && item.first
  end

  def self.current_task(tasks)
    item = tasks.reverse.find { |t| t.size > 1 }
    item && item.first
  end

  def self.strip_prefix(str, prefix)
    s = prefix.size
    if str[0,s] == prefix
      str[s, str.size - s]
    end
  end

  STATS_SLICE = 10 # seconds
  STATS_CHUNK = 90 # slices (= 900 seconds = 15 minutes)
  def self.slice_timestamp(ts)
    slice = ts.to_i / STATS_SLICE
    return slice / STATS_CHUNK, slice % STATS_CHUNK
  end
  def self.enc36(i)
    i.to_s(36)
  end
  def self.dec36(s)
    s.to_i(36)
  end
  def self.enc36_slice_timestamp(ts)
    self.slice_timestamp(ts).map { |i| self.enc36(i) }
  end
  def self.assemble_timestamp(chunk, slice)
    (chunk * STATS_CHUNK + slice) * STATS_SLICE
  end
  def self.dec36_assemble_timestamp(echunk, eslice)
    self.assemble_timestamp(self.dec36(echunk), self.dec36(eslice))
  end

  # Lua script ENQUEUE_JOB takes keys
  # [tasks_h, args_h, jobstat_h, task1_z, queues_h, uniq_h]
  # and args [tasks, args, run_at, uniq].
  # uniq is optional.  If it's present, the script first checks to see
  # if the key uniq is already set in the hash uniq_h.  If so, the
  # negative of the integer value therein is returned and the script does
  # nothing.  Otherwise, an integer id is written as that value, the
  # tasks_h hash gets the value of tasks (JSON-encoded task list)
  # written under the key id, the args_h hash gets the value args
  # written under the key id, task1_z gets id zadded with score
  # run_at.  Also, task1_z is written to jobstatus_h under the key id.
  # The value returned from the operation is the id.  A successful
  # enqueueing is thus signaled by the return of the job id, while an
  # enqueueing blocked by the uniq_in_flight constraint returns minus
  # the blocking id.
  ENQUEUE_JOB = <<EOF
local id = redis.call('hincrby', KEYS[6], '', 1)
if ARGV[4] then
  local ukey = 'u:' .. ARGV[4]
  local uniq = redis.call('hsetnx', KEYS[6], ukey, id)
  if 0 == uniq then
    return (- redis.call('hget', KEYS[6], ukey))
  else
    redis.call('hset', KEYS[6], id, ukey)
  end
end
redis.call('hset', KEYS[1], id, ARGV[1])
redis.call('hset', KEYS[2], id, ARGV[2])
redis.call('hset', KEYS[3], id, KEYS[4] .. ',' .. ARGV[3])
redis.call('zadd', KEYS[4], ARGV[3], id)
redis.call('hset', KEYS[5], KEYS[4], id .. ',' .. ARGV[3])
return id
EOF
  ENQUEUE_JOB_SHA = Digest::SHA1.hexdigest(ENQUEUE_JOB)

  def enqueue(tasks, args, uniq_in_flight = nil, run_at = Time.now.to_f)
    next_task = self.class.next_task(tasks)
    return false, nil unless next_task
    queue_key = @que_prefix + next_task.to_s
    keys = [@tasks, @args, @status, queue_key, @queues, @unique]
    argv = [tasks.to_json, args.to_json, run_at]
    argv << uniq_in_flight if uniq_in_flight
    result_id = redis_eval(ENQUEUE_JOB_SHA, ENQUEUE_JOB, keys, argv)
    return result_id > 0, result_id.abs.to_s
  end

  # Lua script DEQUEUE_TASK takes keys
  #   [args_h, tasks_h, questats_h, retries_h, task1_z, ...],
  # and args [timestamp_f, retry_delay, tmod].
  # It performs a conditional ZPOP on task1_z, where the
  # condition is that the score of the first item is <= timestamp_f.
  # If nothing is available to ZPOP, it tries task2_z, etc.  If an id
  # is returned from any ZPOP, it increments the retry count in retries_h
  # and reschedules the task accordingly.  Then it writes stats info to
  # questats_h, and returns the job information from args_h and tasks_h.

  DEQUEUE_TASK = <<EOF
local ts = tonumber(ARGV[1])
local i = 5
while KEYS[i] do
  local elem = redis.call('zrange', KEYS[i], 0, 0, 'withscores')
  if elem[2] and tonumber(elem[2]) < ts then
    local retries = redis.call('hincrby', KEYS[4], elem[1] .. ',' .. KEYS[i], 1)
    local t_retry = ts + ARGV[2] * 2 ^ retries
    redis.call('zadd', KEYS[i], t_retry, elem[1])
    redis.call('hset', KEYS[3], KEYS[i] .. ',len,' .. ARGV[3],
      redis.call('zcard', KEYS[i]))
    redis.call('hincrby', KEYS[3], KEYS[i] .. ',dq,' .. ARGV[3], 1)
    redis.call('expire', KEYS[3], 21600)
    return({ elem[1],
      redis.call('hget', KEYS[1], elem[1]),
      redis.call('hget', KEYS[2], elem[1]), retries })
  end
  i = i + 1
end
EOF
  DEQUEUE_TASK_SHA = Digest::SHA1.hexdigest(DEQUEUE_TASK)

  def dequeue(tasks, retry_delay = RETRY_DELAY, timestamp = Time.now.to_f)
    stats_chunk, stats_slice = self.class.enc36_slice_timestamp(timestamp)
    questats_key = @questats_prefix + stats_chunk
    keys = [@args, @tasks, questats_key, @retries]
    tasks.each { |task| keys << @que_prefix + task.to_s }
    result = redis_eval(
      DEQUEUE_TASK_SHA, DEQUEUE_TASK, keys,
      [timestamp, retry_delay, stats_slice])
    if result
      id, args, tasks, retries = result
      { id: id, args: JSON.parse(args), tasks: JSON.parse(tasks),
        retries: retries }
    end
  end

  SUCCEED_TASK = <<EOF
local id = ARGV[1]
local ts = ARGV[2]
local tasks = cjson.decode(redis.call('hget', KEYS[1], id))
local cur_task = ''
for _, task in ipairs(tasks) do
  if not task[2] then cur_task = task[1]; break end
end
if (not (string.sub(KEYS[7], - string.len(cur_task)) == cur_task)) then
  return redis.error_reply(
    string.format('task mis-match %s %s %s', id, cur_task, KEYS[7]))
end
if (redis.call('hdel', KEYS[3], id .. ',' .. KEYS[7]) > 0) then
  redis.call('zrem', KEYS[7], id)
else
  return redis.error_reply('missing retry count %s %s', id, KEYS[7])
end
local status = redis.call('hget', KEYS[2], id)
local i = 0
for r in string.gmatch(status, "[^,]+") do
  i = i + 1
  if (i > 2 and i % 2 == 1) then
    local rkey = id .. ',' .. KEYS[7] .. ',' .. r
    redis.call('zrem', KEYS[5], rkey)
    redis.call('hdel', KEYS[6], rkey)
  end
end
redis.call('hset', KEYS[1], id, ARGV[3])
redis.call('hdel', KEYS[3], id .. ',' .. KEYS[4])
redis.call('zadd', KEYS[4], ts, id)
if (KEYS[8]) then
  redis.call('hset', KEYS[2], id, KEYS[8] .. ',' .. ts)
  redis.call('hdel', KEYS[3], id .. ',' .. KEYS[8])
  redis.call('zadd', KEYS[8], ts, id)
  redis.call('hset', KEYS[9], KEYS[8], id .. ',' .. ts)
else
  redis.call('hset', KEYS[2], id, '_' .. ',' .. ts)
end
return id
EOF
  SUCCEED_TASK_SHA = Digest::SHA1.hexdigest(SUCCEED_TASK)

  def succeed(id, tasks, timestamp = Time.now.to_f)
    current_task = self.class.current_task(tasks)
    next_task = self.class.next_task(tasks)
    keys = [
      @tasks, @status, @retries, @report_queue, @failz, @failures,
      @que_prefix + current_task]
    argv = [id, timestamp, tasks.to_json]
    keys << (@que_prefix + next_task) << @queues if next_task
    id == redis_eval(SUCCEED_TASK_SHA, SUCCEED_TASK, keys, argv)
  end

  FAIL_TASK = <<EOF
local id = ARGV[1]
local ts = ARGV[2]
local tasks = cjson.decode(redis.call('hget', KEYS[1], id))
local cur_task = ''
for _, task in ipairs(tasks) do
  if not task[2] then cur_task = task[1]; break end
end
if (not (string.sub(ARGV[3], - string.len(cur_task)) == cur_task)) then
  return redis.error_reply(
    string.format('task mismatch %s %s %s', id, cur_task, ARGV[3]))
end
local rkey = id .. ',' .. ARGV[3]
local retries = tonumber(redis.call('hget', KEYS[3], rkey))
if (not retries) then
  return redis.error_reply(
    string.format('missing retry count %s %s', id, ARGV[3]))
end
rkey = rkey .. ',' .. retries
redis.call('zadd', KEYS[4], ts, rkey)
redis.call('hset', KEYS[5], rkey, ARGV[4])
local status = redis.call('hget', KEYS[2], id)
status = status .. ',' .. retries .. ',' .. ts
redis.call('hset', KEYS[2], id, status)
redis.call('hdel', KEYS[3], id .. ',' .. KEYS[6])
redis.call('zadd', KEYS[6], ts, id)
return id
EOF
  FAIL_TASK_SHA = Digest::SHA1.hexdigest(FAIL_TASK)

  def fail(id, task, details, timestamp = Time.now.to_f)
    keys = [@tasks, @status, @retries, @failz, @failures, @report_queue]
    argv = [id, timestamp, @que_prefix + task, JSON.generate(details)]
    id == redis_eval(FAIL_TASK_SHA, FAIL_TASK, keys, argv)
  end

  def get_failures(failz)
    result = Hash.new { Array.new }
    fkeys = failz.keys
    if fkeys.size > 0
      failures = redis.hmget(@failures, fkeys)
      fkeys.zip(failures).each do |key, details|
        id, queue, r = key.split(',')
        r = r.to_i
        task = self.class.strip_prefix(queue, @que_prefix)
        result[id] <<= {
          task: task, retries: r, ts: failz[key],
          details: JSON.parse(details) }
      end
    end
    result
  end

  def get_failz(earliest = 0, latest = Time.now.to_f, limit = -100)
    values =
      if limit < 0
        redis.zrevrangebyscore(
        @failz, latest, earliest, limit: [0, -limit], with_scores: true)
      else
        redis.zrangebyscore(
        @failz, earliest, latest, limit: [0, limit], with_scores: true)
      end
    result = {}
    values.each { |v| result[v[0]] = v[1] }
    result
  end

  def failures(*args)
    get_failures(get_failz(*args))
  end

  def delete_queue(queue)
    queue_key = @que_prefix + queue.to_s
    redis.multi do |r|
      r.del(queue_key)
      r.hdel(@queues, queue_key)
    end.last == 1
  end

  def queues
    result = redis.hgetall(@queues)
    result.keys.map { |k| self.class.strip_prefix(k, @que_prefix) }
  end

  def queue_length(queue)
    redis.zcard(@que_prefix + queue) || 0
  end

  def decode_job_status(status)
    queue, ts, *retries = status.split(',')
    ts = ts.to_f
    timestamps = [ts]
    while retries.size > 0
      i = retries.shift.to_i
      timestamps[i] = retries.shift.to_f
    end
    return queue, timestamps
  end

  def remove_job(id)
    status = redis.hget(@status, id)
    queue, timestamps = decode_job_status(status)
    redis.multi do |r|
      if queue.start_with?(@que_prefix)
        r.zrem(queue, id)
        rkey = "#{id},#{queue}"
        r.hdel(@retries, rkey)
        rkeys = []
        timestamps.drop(1).each_with_index do |ts, i|
          rkeys << rkey + ",#{i + 1}"
        end
        if rkeys.size > 0
          r.hdel(@failures, rkeys)
          r.zrem(@failz, rkeys)
        end
      end
      r.hdel(@args, id)
      r.hdel(@tasks, id)
    end
    check_status = redis.hget(@status, id)
    return if check_status.nil?
    if check_status == status
      redis.hdel(@status, id)
      if (uid = redis.hget(@unique, id))
        redis.hdel(@unique, [id, uid])
      end
    else
      remove_job(id)
    end
  end

  def job_status(*ids)
    statuses = redis.hmget(@status, *ids)
    result = {}
    ids.zip(statuses).each do |(id, status)|
      next unless status
      queue, timestamps = decode_job_status(status)
      result[id] = {
        task: self.class.strip_prefix(queue, @que_prefix),
        timestamps: timestamps }
    end
    result
  end

  def fill_job_failures(statuses)
    failz = {}
    statuses.each do |id, status|
      next unless (task = status[:task])
      timestamps = status[:timestamps]
      next unless timestamps.size > 1
      queue = @que_prefix + task
      timestamps.drop(1).each_with_index do |ts, i|
        failz["#{id},#{queue},#{i+1}"] = ts
      end
    end
    get_failures(failz).each do |id, failures|
      statuses[id][:failures] = failures
    end
    statuses
  end

  def fill_args_tasks(statuses)
    ids = statuses.keys
    args, tasks = redis.multi do |r|
      r.hmget(@args, ids)
      r.hmget(@tasks, ids)
    end
    ids.zip(args, tasks).each do |id, a, t|
      statuses[id][:args] = a && JSON.parse(a)
      statuses[id][:tasks] = t && JSON.parse(t)
    end
  end

  def queue_stats(chunks = 3, latest = Time.now.to_f)
    last_chunk, last_slice = self.class.slice_timestamp(latest)
    stats = {}
    (0..(chunks - 1)).each do |chunk_i|
      chunk_ts = self.class.enc36(last_chunk - chunk_i)
      questats_key = @questats_prefix + chunk_ts
      stats_chunk = redis.hgetall(questats_key)
      next unless stats_chunk
      stats_chunk.each do |key, val|
        queue, stat, slice = key.split(',')
        queue = self.class.strip_prefix(queue, @que_prefix)
        timestamp = self.class.dec36_assemble_timestamp(chunk_ts, slice)
        stats[stat] = {} unless stats[stat]
        stats[stat][timestamp] = {} unless stats[stat][timestamp]
        stats[stat][timestamp][queue] = val.to_i
      end
    end
    stats
  end

  def push_report(id, timestamp = Time.now.to_f)
    redis.multi do |r|
      r.hdel(@retries, "#{id},#{@report_queue}")
      r.zadd(@report_queue, timestamp, id)
    end
  end

  REPORT_POP = <<EOF
local t_pop = tonumber(ARGV[1])
local elem = redis.call('zrange', KEYS[1], 0, 0, 'withscores')
local t_elem = tonumber(elem[2])
if (t_elem and t_elem < t_pop) then
  local retries = redis.call('hincrby', KEYS[2], elem[1] .. ',' .. KEYS[1], 1)
  local t_retry = t_pop + tonumber(ARGV[2]) * 2 ^ retries
  redis.call('zadd', KEYS[1], t_retry, elem[1])
  elem[3] = retries
  elem[2] = t_elem
  return cjson.encode(elem)
end
EOF
  REPORT_POP_SHA = Digest::SHA1.hexdigest(REPORT_POP)

  def pop_report(timestamp = Time.now.to_f)
    result = redis_eval(
      REPORT_POP_SHA, REPORT_POP, [@report_queue, @retries],
      [timestamp, REPORT_RETRY_DELAY])
    result &&= JSON.parse(result)
  end

  REPORT_COMPLETE = <<EOF
if (redis.call('hdel', KEYS[2], ARGV[1] .. ',' .. KEYS[1]) > 0) then
  redis.call('zrem', KEYS[1], ARGV[1])
end
EOF
  REPORT_COMPLETE_SHA = Digest::SHA1.hexdigest(REPORT_COMPLETE)
  def complete_report(id)
    redis_eval(REPORT_COMPLETE_SHA, REPORT_COMPLETE,
      [@report_queue, @retries], [id])
  end

  def put_env(topic, h)
    return if h.empty?
    kvs = []
    h.each { |k, v| kvs << k << v }
    hkey = @env_prefix + topic.to_s
    'OK' == redis.hmset(hkey, *kvs)
  end

  def rm_env(topic, keys = nil)
    hkey = @env_prefix + topic.to_s
    if keys.nil?
      redis.del(hkey)
    elsif !keys.empty?
      redis.hdel(hkey, keys)
    end
  end

  def get_env(topic, keys = nil)
    hkey = @env_prefix + topic.to_s
    if keys.nil?
      redis.hgetall(hkey)
    elsif keys.empty?
      {}
    else
      result = {}
      values = redis.hmget(hkey, keys)
      keys.zip(values).each { |k, v| result[k] = v }
      result
    end
  end
end
