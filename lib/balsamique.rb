require 'securerandom'
require 'digest/sha1'
require 'json'
require 'redis'

class Balsamique
  def initialize(redis, namespace = 'bQ')
    @redis = redis

    @que_prefix = namespace + ':que:'
    @working_prefix = namespace + ':working:'

    @jobstatus = namespace + ':jobstatus'
    @failz = namespace + ':failz'
    @workers = namespace + ':workers'
    @queues = namespace + ':queues'
    @unique = namespace + ':unique'
    @tasks = namespace + ':tasks'
    @args = namespace + ':args'
  end

  def redis
    @redis
  end

  def redis_eval(cmd_sha, cmd, keys, argv)
    begin
      redis.evalsha(cmd_sha, keys, argv)
    rescue Redis::CommandError
      puts "[INFO] Falling back to EVAL for #{cmd_sha}"
      redis.eval(cmd, keys, argv)
    end
  end

  def self.next_task(tasks)
    item = tasks.find { |t| t.size == 1 }
    item && item.first
  end

  def self.strip_prefix(str, prefix)
    s = prefix.size
    if str[0,s] == prefix
      str[s, str.size - s]
    end
  end

  def self.match_prefix(str, matches)
    matches.each do |prefix, proc|
      stripped = self.strip_prefix(str, prefix)
      return proc.call(stripped) if stripped
    end
    return nil
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
redis.call('zadd', KEYS[4], tonumber(ARGV[3]), id)
redis.call('hset', KEYS[5], KEYS[4], id .. ',' .. ARGV[3])
return id
EOF
  ENQUEUE_JOB_SHA = Digest::SHA1.hexdigest(ENQUEUE_JOB)
  def enqueue(tasks, args, uniq_in_flight = nil, run_at = Time.now.to_f)
    #    validate_tasks!(tasks)
    #    validate_args!(args)
    next_task = self.class.next_task(tasks)
    return false, false unless next_task
    queue_key = @que_prefix + next_task
    keys = [@tasks, @args, @jobstatus, queue_key, @queues, @unique]
    argv = [tasks.to_json, args.to_json, run_at]
    argv << uniq_in_flight if uniq_in_flight
    result_id = redis_eval(ENQUEUE_JOB_SHA, ENQUEUE_JOB, keys, argv)
    return result_id > 0, result_id.abs.to_s
  end

  # Lua script DEQUEUE_TASK takes keys
  # [task_z, working_l, args_h, jobstat_h, tasks_h, workers_h]
  # and args [timestamp_f].
  # It performs a conditional ZPOP on task_z, where the condition
  # is that the score of the first item is <= timestamp_f.
  # If nothing is available to ZPOP, the script is done.
  # Otherwise, it lpushes the popped id onto working_l,
  # updates jobstat_h to point to working_l with timestamp_f, and
  # returns the job information from args_h and tasks_h.
  DEQUEUE_TASK = <<EOF
local ts = tonumber(ARGV[1])
redis.call('hset', KEYS[5], KEYS[1], ts)
local qi = 6
while KEYS[qi] do
  local elem = redis.call('zrange', KEYS[qi], 0, 0, 'withscores')
  if elem[2] and tonumber(elem[2]) <= ts then
    redis.call('zrem', KEYS[qi], elem[1])
    redis.call('lpush', KEYS[1], elem[1])
    redis.call('hset', KEYS[2], elem[1],
      KEYS[1] .. ',' .. KEYS[qi] .. ',' .. ts)
    return({ elem[1],
      redis.call('hget', KEYS[3], elem[1]),
      redis.call('hget', KEYS[4], elem[1]) })
  end
  qi = qi + 1
end
EOF
  DEQUEUE_TASK_SHA = Digest::SHA1.hexdigest(DEQUEUE_TASK)
  def dequeue(tasks, worker, timestamp = Time.now.to_f)
    working_key = @working_prefix + worker.to_s
    keys = [working_key, @jobstatus, @args, @tasks, @workers]
    tasks.each { |task| keys << @que_prefix + task.to_s }
    result = redis.eval(DEQUEUE_TASK, keys, [timestamp])
    if result
      id, args, tasks = result
      { id: id, args: JSON.parse(args), tasks: JSON.parse(tasks) }
    end
  end

  SUCCEED_TASK = <<EOF
local ts = tonumber(ARGV[2])
local id = redis.call('rpop', KEYS[1])
while id and not(id == ARGV[1]) do
  local tasks = cjson.decode(redis.call('hget', KEYS[4], id))
  local cur_task = ''
  for _, task in ipairs(tasks) do
    if not task[2] then cur_task = task[1]; break end
  end
  redis.call('zadd', KEYS[3], ts, id)
  redis.call('hset', KEYS[2], id,
    KEYS[3] .. ',' .. cur_task .. ',' .. KEYS[1] .. ',' .. ts .. ',' ..
     '{"message":"Lost State"}')
  id = redis.call('rpop', KEYS[1])
end
if id then
  if ARGV[3] then
    redis.call('zadd', KEYS[5], ts, id)
    redis.call('hset', KEYS[2], id, KEYS[5] .. ',' .. ts)
    redis.call('hset', KEYS[4], id, ARGV[3])
    redis.call('hset', KEYS[6], KEYS[5], id .. ',' .. ts)
  else
    redis.call('hdel', KEYS[2], id)
    redis.call('hdel', KEYS[4], id)
    redis.call('hdel', KEYS[5], id)
    local ukey = redis.call('hget', KEYS[6], id)
    if ukey then
      redis.call('hdel', KEYS[6], ukey)
      redis.call('hdel', KEYS[6], id)
    end
  end
end
return id
EOF
  SUCCEED_TASK_SHA = Digest::SHA1.hexdigest(SUCCEED_TASK)
  def succeed(id, worker, tasks, timestamp = Time.now.to_f)
    next_task = self.class.next_task(tasks)
    working = @working_prefix + worker.to_s
    keys = [working, @jobstatus, @failz, @tasks]
    argv = [id, timestamp]
    if next_task
      argv << tasks.to_json
      keys << (@que_prefix + next_task) << @queues
    else
      keys << @args << @unique
    end
    id == redis_eval(SUCCEED_TASK_SHA, SUCCEED_TASK, keys, argv)
  end

  FAIL_TASK = <<EOF
local ts = tonumber(ARGV[2])
local id = redis.call('rpop', KEYS[1])
while id do
  local tasks = cjson.decode(redis.call('hget', KEYS[4], id))
  local cur_task = ''
  for _, task in ipairs(tasks) do
    if not task[2] then cur_task = task[1]; break end
  end
  redis.call('zadd', KEYS[3], ts, id)
  local reason = '{"message":"Lost State"}'
  if ARGV[1] == id then reason = ARGV[3] end
  redis.call('hset', KEYS[2], id,
    KEYS[3] .. ',' .. cur_task .. ',' .. KEYS[1] .. ',' .. ts .. ',' .. reason)
  if ARGV[1] == id then return id end
  id = redis.call('rpop', KEYS[1])
end
EOF
  FAIL_TASK_SHA = Digest::SHA1.hexdigest(FAIL_TASK)
  def fail(id, worker, reason, timestamp = Time.now.to_f)
    working = @working_prefix + worker.to_s
    keys = [working, @jobstatus, @failz, @tasks]
    argv = [id, timestamp, JSON.generate(reason)]
    id == redis_eval(FAIL_TASK_SHA, FAIL_TASK, keys, argv)
  end

  def queues
    result = redis.hgetall(@queues)
    result.keys.map { |k| self.class.strip_prefix(k, @que_prefix) }
  end

  def workers
    result = redis.hgetall(@workers)
    result.keys.map { |k| self.class.strip_prefix(k, @working_prefix) }
  end

  def queue_length(queue)
    redis.zcard(@que_prefix + queue) || 0
  end

  def job_status(*ids)
    statuses = redis.hmget(@jobstatus, *ids)
    result = {}
    ids.zip(statuses).each do |(id, status)|
      result[id] = self.class.match_prefix(status,
        { @que_prefix => Proc.new do |s|
            task, run_at = s.split(',')
            { state: :enqueued, task: task, run_at: run_at.to_f }
          end,
          @working_prefix => Proc.new do |s|
            worker, queue_key, ts = s.split(',')
            task = self.class.strip_prefix(queue_key, @que_prefix)
            { state: :working, task: task, worker: worker, ts: ts.to_f }
          end,
          @failz + ',' => Proc.new do |s|
            task, working, ts, reason = s.split(',', 4)
            reason = JSON.parse(reason)
            worker = self.class.strip_prefix(working, @working_prefix)
            { state: :failed, task: task, worker: worker, ts: ts.to_f,
              reason: reason }
          end
        })
    end
    result
  end
end
