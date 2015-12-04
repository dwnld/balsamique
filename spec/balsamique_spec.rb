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
    end
  end

  it 'allows a worker to record a task failure' do
    t0 = Time.now.to_f
    queued, id = @bq.enqueue(tasks, args, nil, t0)
    task = tasks.first.first
    job = @bq.dequeue([task])
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
    @bq.remove_job(id)
    expect(@bq.failures).to eq({})
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
end
