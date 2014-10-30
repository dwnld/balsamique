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
  end

  it 'records a lost state failure when a worker drops a task' do
    queued, id = @bq.enqueue(tasks, args)
    task = tasks.first.first
    job = @bq.dequeue([task], worker)
    expect(@bq.succeed(0, worker, [['foo', 'bar']])).to eq(false)
    expect(@bq.job_status(id)[id]).to include(
      { state: :failed, task: task, worker: worker,
        reason: { 'message' => 'Lost State' } })
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
end
