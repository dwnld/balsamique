require 'bundler'
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
  let(:worker) { random_name }
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
      expect(job_status[id]).to include({ state: :enqueued, task: task.first })

      job = @bq.dequeue(queues.shuffle, worker)
      expect(job).to eq({id: id, args: args, tasks: mtasks})
      job_status = @bq.job_status(id)
      expect(job_status.keys).to eq([id])
      expect(job_status[id]).to include(
        { state: :working, task: task.first, worker: worker })

      expect(@bq.workers).to eq([worker])

      mtasks[i] << random_name # Record output of task
      expect(@bq.succeed(id, worker, mtasks)).to eq(true)
    end
  end

  it 'allows a worker to record a task failure' do
    queued, id = @bq.enqueue(tasks, args)
    task = tasks.first.first
    job = @bq.dequeue([task], worker)
    fail_reason = { 'message' => 'Test Fail' }
    expect(@bq.fail(id, worker, fail_reason)).to eq(true)
    expect(@bq.queue_length(task)).to eq(0)
    job_status = @bq.job_status(id)
    expect(job_status[id]).to include(
      { state: :failed, task: task, worker: worker, reason: fail_reason })
    expect(@bq.get_failures).to eq([id])
  end

  it 'records a lost state failure when a worker drops a task' do
    queued, id = @bq.enqueue(tasks, args)
    task = tasks.first.first
    job = @bq.dequeue([task], worker)
    expect(@bq.succeed(0, worker, [['foo', 'bar']])).to eq(false)
    expect(@bq.job_status(id)[id]).to include(
      { state: :failed, task: task, worker: worker,
        reason: { 'message' => 'Lost State' } })
    expect(@bq.get_failures).to eq([id])
  end

  it 'records a lost state failure when worker drops a task, fails another' do
    queued, id = @bq.enqueue(tasks, args)
    task = tasks.first.first
    job = @bq.dequeue([task], worker)
    expect(@bq.fail(0, worker, {})).to eq(false)
    expect(@bq.job_status(id)[id]).to include(
      { state: :failed, task: task, worker: worker,
        reason: { 'message' => 'Lost State' } })
  end

  it 'allows a worker to retire, recording any lost state failures' do
    queued, id = @bq.enqueue(tasks, args)
    task = tasks.first.first
    job = @bq.dequeue(tasks.map { |t| t.first }, worker)
    expect(@bq.workers).to eq([worker])
    expect(@bq.retire_worker(worker)).to eq(true)
    expect(@bq.workers).to eq([])
    expect(@bq.job_status(id)[id]).to include(
      { state: :failed, task: task, worker: worker,
        reason: { 'message' => 'Lost State' } })
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
    job = @bq.dequeue([task], worker)
    @bq.succeed(id, worker, [[task, true]])
    queue2, id2 = @bq.enqueue(tasks.take(1), args, 'bar')
    expect(queue2).to eq(true)
    expect(id2).not_to eq(id)
  end

  it 'allows a failed task to be retried' do
    queued, id = @bq.enqueue(tasks, args)
    task = tasks.first.first
    job0 = @bq.dequeue([task], worker)
    @bq.fail(id, worker, { 'message' => 'Test Fail' })
    @bq.retry(id, task)
    expect(@bq.queue_length(task)).to eq(1)
    job1 = @bq.dequeue([task], worker)
    expect(job1).to eq(job0)
    expect(@bq.job_status(id)[id]).to include(
      { state: :working, task: task, worker: worker })
    expect(@bq.get_failures).to eq([])
  end

  it 'allows a failed task to be cleared' do
    queued, id = @bq.enqueue(tasks, args, 'bar')
    task = tasks.first.first
    job = @bq.dequeue([task], worker)
    @bq.fail(id, worker, { 'message' => 'Test Fail' })
    expect(@bq.get_failures).to eq([id])
    @bq.clear_failed_jobs(id)
    expect(@bq.get_failures).to eq([])
    expect(@bq.job_status(id)).to eq({})
    queue2, id2 = @bq.enqueue(tasks, args, 'bar')
    expect(queue2).to eq(true)
    expect(id2).not_to eq(id)
  end

  it 'counts task dequeue events in queue_stats' do
    @bq.enqueue(tasks, args)
    @bq.enqueue(tasks, args)
    t0 = Time.now.to_f
    queues = tasks.map{ |task| task.first }
    mtasks = tasks
    job0 = @bq.dequeue(queues, worker, t0)
    job1 = @bq.dequeue(queues, worker, t0)
    mtasks[0] << true
    @bq.succeed(job0[:id], worker, mtasks)
    @bq.succeed(job1[:id], worker, mtasks)
    job0 = @bq.dequeue(queues, worker, t0 + Balsamique::STATS_SLICE)

    slice0 = t0.to_i
    slice0 -= slice0 % Balsamique::STATS_SLICE
    slice1 = slice0 + Balsamique::STATS_SLICE

    expect(@bq.queue_stats(3, t0 + 2 * Balsamique::STATS_SLICE))
      .to eq({
        "len" => {slice0 => {queues[0] => 0}, slice1 => {queues[1] => 1}},
        "dq"  => {slice0 => {queues[0] => 2}, slice1 => {queues[1] => 1}}})
  end

  it 'allows pushing, popping report queue messages' do
    timestamp = Time.now.to_f
    pop_time = timestamp + 0.001
    id = random_name
    @bq.push_report(id, timestamp)
    expect(@bq.pop_report(timestamp - 0.001)).to be nil
    expect(@bq.pop_report(pop_time)).to eq([id, timestamp])
    expect(@bq.pop_report(timestamp + 0.002)).to be nil
    @bq.complete_report(id)
    pop_time +=
      Balsamique::REPORT_RETRY_DELAY *
      2**(Balsamique::REPORT_MAX_RETRIES + 3)
    expect(@bq.pop_report(pop_time)).to be nil
  end

  it 'pops two report queue messages under race condition' do
    timestamp = Time.now.to_f
    pop_time = timestamp + 0.001
    id = random_name
    @bq.push_report(id, timestamp)
    expect(@bq.pop_report(pop_time)).to eq([id, timestamp])
    @bq.push_report(id, pop_time)
    @bq.complete_report(id)
    expect(@bq.pop_report(pop_time + 0.001)).to eq([id, pop_time])
    @bq.complete_report(id)
    pop_time +=
      Balsamique::REPORT_RETRY_DELAY *
      2**(Balsamique::REPORT_MAX_RETRIES + 3)
    expect(@bq.pop_report(pop_time)).to be nil
  end

  it 'retries report queue messages with exponential backoff' do
    timestamp = Time.now.to_f
    pop_time = timestamp + 0.001
    id = random_name
    @bq.push_report(id, timestamp)
    expect(@bq.pop_report(pop_time)).to eq([id, timestamp])
    (1..Balsamique::REPORT_MAX_RETRIES).each do |count|
      pop_time += 0.001 + Balsamique::REPORT_RETRY_DELAY * 2**count
      result = @bq.pop_report(pop_time)
      expect(result.size).to eq(2)
      expect(result.first).to eq(id)
      expect((result.last - pop_time + 0.001).abs).to be < 0.0001
    end
    pop_time +=
      Balsamique::REPORT_RETRY_DELAY *
      2**(Balsamique::REPORT_MAX_RETRIES + 2)
    expect(@bq.pop_report(pop_time)).to be nil
  end
end
