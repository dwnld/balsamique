require 'bundler'
require 'securerandom'
Bundler.require(:default, :test)

describe Balsamique do
  def random_name
    ('a'..'z').to_a.shuffle[0,6].join
  end
  before :all do
    redis = Redis.connect(url: 'redis://localhost:6379/7')
    @bq = Balsamique.new(redis)
  end
  before :each do
    @bq.redis.flushdb
  end
  after :all do
    @bq.redis.flushdb
  end

  let(:tasks) { (1..3).map { |i| [random_name] } }
  let(:args) { { 'time' => Time.now.to_i, 'rand' => SecureRandom.base64(6) } }

  it 'allows processing a job through successive tasks' do
    queued, id = @bq.enqueue(tasks, args)
    expect(queued).to eq(true)

    queues = tasks.map { |task| task.first }
    mtasks = tasks.map {|t| t.dup }
    mtasks.each_with_index do |task, i|
      expect(@bq.queues).to eq(queues.take(i + 1))
      expect(@bq.queue_length(queues[i])).to eq(1)
      expect(@bq.queues_info.keys).to eq(@bq.queues)
      @bq.queues_info.each do |queue, info|
        if queue == queues[i]
          expect(info).to match(
            current_ts: Float, last_id: id, last_ts: Float,
            total: 1, ready: 1, next_id: id, next_ts: Float)
        else
          expect(info).to match(
            current_ts: Float, last_id: id, last_ts: Float,
            total: 0, ready: 0, next_id: nil, next_ts: nil)
        end
      end
      job_status = @bq.job_status(id)
      expect(job_status.keys).to eq([id])
      expect(job_status[id]).to include({ task: task.first })

      job = @bq.dequeue(queues.shuffle)
      expect(job).to eq({ id: id, args: args, tasks: mtasks, retries: 1 })
      job_status = @bq.job_status(id)
      expect(job_status.keys).to eq([id])
      expect(job_status[id]).to include({ task: task.first })

      mtasks[i] << random_name # Record output of task
      expect(@bq.succeed(id, mtasks)).to eq(true)
      expect(@bq.pop_report).to match([id, Float, 1])
      @bq.complete_report(id)
      expect(@bq.pop_report).to be nil
    end
    expect(@bq.queues.all? { |q| @bq.queue_length(q) == 0 }).to eq(true)
    expect(@bq.job_status(id)[id]).to include({ task: nil })
    @bq.remove_job(id)
    expect(@bq.job_status(id)).to eq({})
  end

  it 'allows a worker to record a task failure' do
    t0 = Time.now.to_f
    _queued, id = @bq.enqueue(tasks, args, nil, t0)
    task = tasks.first.first
    job = @bq.dequeue([task])
    expect(job).to eq({ id: id, args: args, tasks: tasks, retries: 1 })
    t1 = Time.now.to_f
    fail_reason = { 'message' => 'Test Fail' }
    expect(@bq.fail(id, task, fail_reason, t1)).to eq(true)
    job_status = @bq.job_status(id)
    expect(job_status[id]).to eq({ task: task, timestamps: [t0, t1] })
    failures = @bq.failures
    expect(failures).to eq({
        id => [{ task: task, retries: 1, ts: t1, details: fail_reason }] })
    @bq.fill_job_failures(job_status)
    expect(job_status[id][:failures]).to eq(failures[id])
    report = @bq.pop_report
    expect(report).to match([id, Float, 1])
    expect(report[1]).to be_within(0.0001).of(t1)
    @bq.complete_report(id)
    expect(@bq.pop_report).to be nil
    @bq.remove_job(id)
    expect(@bq.failures).to eq({})
  end

  it 'automatically retries tasks with exponential backoff' do
    retry_delay = Balsamique::RETRY_DELAY * 3
    t0 = Time.now.to_f
    pop_time = t0 + 0.001
    timestamps = [t0]
    _queued, id = @bq.enqueue(tasks, args, nil, t0)
    task = tasks.first.first
    job = @bq.dequeue([task], retry_delay, pop_time)
    expect(job).to eq({ id: id, args: args, tasks: tasks, retries: 1 })
    failure = { 'message' => 'we broke it', 'times' => 1 }
    timestamps << (pop_time + 1)
    @bq.fail(id, task, failure, pop_time + 1)
    job_status = @bq.job_status(id)
    @bq.fill_job_failures(job_status)
    expect(job_status).to match(
      id => { task: task, timestamps: timestamps, failures: Array })
    expect(job_status[id][:failures].size).to eq(1)
    expect(job_status[id][:failures].last).to include(
      task: task, details: failure, ts: timestamps.last, retries: 1)
    report = @bq.pop_report(timestamps.last + 1)
    expect(report).to match ([id, Float, 1])
    expect(report[1]).to be_within(0.0001).of(timestamps.last)

    (1..6).each do |count|
      pop_time += retry_delay * 2**count - 0.001
      expect(@bq.dequeue([task], retry_delay, pop_time)).to be nil
      pop_time += 0.002
      job = @bq.dequeue([task], retry_delay, pop_time)
      expect(job).to eq({ id: id, args: args, tasks: tasks, retries: count + 1})
      if (1 == count % 2)
        failure = { 'message' => 'we broke it', 'times' => count + 1 }
        timestamps << (pop_time + 1)
        @bq.fail(id, task, failure, pop_time + 1)
      end
      job_status = @bq.job_status(id)
      @bq.fill_job_failures(job_status)
      expect(job_status).to match(
        id => { task: task, timestamps: timestamps, failures: Array })
      expect(job_status[id][:failures].size).to eq(count + count % 2)
      if (1 == count % 2)
        expect(job_status[id][:failures].last).to include(
          task: task, details: failure, ts: timestamps.last, retries: count + 1)
        report = @bq.pop_report(timestamps.last + 1)
        expect(report).to match ([id, Float, 1])
        expect(report[1]).to be_within(0.0001).of(timestamps.last)
      else
        timestamps << nil
      end
    end
  end

  it 'allows a queue to be deleted' do
    queued, id = @bq.enqueue(tasks, args)
    queue = tasks.first.first
    expect(@bq.queues).to eq([queue])
    expect(@bq.delete_queue(queue)).to eq(true)
    expect(@bq.queues).to eq([])
    expect(@bq.queue_length(queue)).to eq(0)
  end

  it 'blocks attempts to enqueue jobs with duplicate unique keys' do
    queued, id = @bq.enqueue(tasks, args, 'foo')
    expect(queued).to eq(true)
    queue2, id2 = @bq.enqueue(tasks.drop(1), {}, 'foo')
    expect(queue2).to eq(false)
    expect(id2).to eq(id)
  end

  it 'retires unique keys when jobs complete successfully' do
    queued, id = @bq.enqueue(tasks.take(1), args, 'bar')
    task = tasks.first.first
    job = @bq.dequeue([task])
    @bq.succeed(id, [[task, true]])
    @bq.remove_job(id)
    queue2, id2 = @bq.enqueue(tasks.take(1), args, 'bar')
    expect(queue2).to eq(true)
    expect(id2).not_to eq(id)
  end

  it 'counts task dequeue events in queue_stats' do
    @bq.enqueue(tasks, args)
    @bq.enqueue(tasks, args)
    t0 = Time.now.to_f
    queues = tasks.map{ |task| task.first }
    mtasks = tasks
    job0 = @bq.dequeue(queues, 600, t0)
    job1 = @bq.dequeue(queues, 600, t0)
    mtasks[0] << true
    @bq.succeed(job0[:id], mtasks)
    @bq.succeed(job1[:id], mtasks)
    job0 = @bq.dequeue(queues, 600, t0 + Balsamique::STATS_SLICE)

    slice0 = t0.to_i
    slice0 -= slice0 % Balsamique::STATS_SLICE
    slice1 = slice0 + Balsamique::STATS_SLICE

    expect(@bq.queue_stats(3, t0 + 2 * Balsamique::STATS_SLICE))
      .to eq({
        "len" => {slice0 => {queues[0] => 2}, slice1 => {queues[1] => 2}},
        "dq"  => {slice0 => {queues[0] => 2}, slice1 => {queues[1] => 1}}})
  end

  it 'allows pushing, popping report queue messages' do
    timestamp = Time.now.to_f
    pop_time = timestamp + 0.001
    id = random_name
    @bq.push_report(id, timestamp)
    expect(@bq.pop_report(timestamp - 0.001)).to be nil
    popped = @bq.pop_report(pop_time)
    expect(popped).to match([id, Float, 1])
    expect(popped[1]).to be_within(0.0001).of(timestamp)
    expect(@bq.pop_report(timestamp + 0.002)).to be nil
    @bq.complete_report(id)
    pop_time += Balsamique::REPORT_RETRY_DELAY * 600
    expect(@bq.pop_report(pop_time)).to be nil
  end

  it 'pops two report queue messages under race condition' do
    timestamp = Time.now.to_f
    pop_time = timestamp + 0.001
    id = random_name
    @bq.push_report(id, timestamp)
    popped = @bq.pop_report(pop_time)
    expect(popped).to match([id, Float, 1])
    expect(popped[1]).to be_within(0.0001).of(timestamp)
    @bq.push_report(id, pop_time)
    @bq.complete_report(id)
    popped = @bq.pop_report(pop_time + 0.001)
    expect(popped).to match([id, Float, 1])
    expect(popped[1]).to be_within(0.0001).of(pop_time)
    @bq.complete_report(id)
    pop_time += Balsamique::REPORT_RETRY_DELAY * 600
    expect(@bq.pop_report(pop_time)).to be nil
  end

  it 'retries report queue messages with exponential backoff' do
    timestamp = Time.now.to_f
    pop_time = timestamp + 0.001
    id = random_name
    @bq.push_report(id, timestamp)
    result = @bq.pop_report(pop_time)
    expect(result).to match([id, Float, 1])
    expect(result[1]).to be_within(0.0001).of(timestamp)

    (1..6).each do |count|
      pop_time += 0.001 + Balsamique::REPORT_RETRY_DELAY * 2**count
      result = @bq.pop_report(pop_time)
      expect(result).to match([id, Float, count + 1])
      expect(result[1]).to be_within(0.0001).of(pop_time - 0.001)
    end
    @bq.complete_report(id)
    pop_time += Balsamique::REPORT_RETRY_DELAY * 600
    expect(@bq.pop_report(pop_time)).to be nil
  end

  it 'allows storing and retrieving env values' do
    topic = random_name
    h = { random_name => '' }
    (1..10).each { |i| h[random_name] = rand(2**i).to_s }
    @bq.put_env(topic, h)
    expect(@bq.get_env(topic)).to eq(h)

    keys = h.keys.sample(5)
    keys << random_name
    h_selected = h.select { |k, _| keys.include?(k) }
    h_selected[keys.last] = nil
    expect(@bq.get_env(topic, keys)).to eq(h_selected)

    h2 = {}
    keys.each { |k| h2[k] = random_name }
    (1..5).each { |i| h2[random_name] = rand(2**i).to_s }
    @bq.put_env(topic, h2)
    h_merged = h.merge(h2)
    expect(@bq.get_env(topic)).to eq(h_merged)

    @bq.rm_env(topic, keys)
    h_reduced = h_merged.select { |k, _| !keys.include? k }
    expect(@bq.get_env(topic)).to eq(h_reduced)

    @bq.rm_env(topic)
    expect(@bq.get_env(topic)).to eq({})
  end

  it 'allows initializing job id floor' do
    id_floors = (1..200).map { |i| rand(10_000_000 * i) }
    id_floors.each { |id_floor| @bq.set_id_floor(id_floor) }
    queued, id = @bq.enqueue(tasks, args)
    expect(id).to eq((id_floors.max + 1).to_s)
  end

  it 'allows peeking at queue contents' do
    timestamp = Time.now.to_i
    queues = tasks.map(&:first)
    q_contents = {}
    (0..99).each do |i|
      ts = timestamp + i * 0.001
      queued, id = @bq.enqueue(tasks, args, nil, ts)
      q_contents[id] = { ts: ts, retries: 0 }
    end
    (0..49).each do |i|
      ts = timestamp + 1 + i * 0.001
      job = @bq.dequeue(queues, 1, ts)
      expect(job[:id]).to eq(q_contents.keys[i])
      q_contents[job[:id]] = { ts: ts + 2, retries: 1 }
    end
    expect(@bq.queue_peek(queues.first, 100)).to eq(q_contents)
  end

  it 'allows accelerating scheduled tasks' do
    timestamp = Time.now.to_i.to_f
    queue = tasks.first.first
    q_contents = {}
    (0..99).each do |i|
      ts = timestamp + i
      queued, id = @bq.enqueue(tasks, args, nil, ts)
      q_contents[id] = { ts: ts, retries: 0 }
    end
    later_timestamp = timestamp + 50
    expect(@bq.accelerate_retries(queue, 0.01, later_timestamp)).to eq 50
    count = 0
    q_contents.each do |id, props|
      if props[:ts] >= later_timestamp
        props[:ts] = later_timestamp + 0.01 * count
        count += 1
      end
    end
    expect(@bq.queue_peek(queue, 100)).to eq(q_contents)
  end
end
