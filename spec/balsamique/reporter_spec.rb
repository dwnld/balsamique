require 'bundler'
require 'securerandom'
Bundler.require(:default, :test)
require 'balsamique/reporter'

describe Balsamique::Reporter do
  class TestLogger
    def initialize
      @infos = []
    end
    attr_accessor :infos
    def debug(msg)
      puts msg.backtrace if msg.is_a? Exception
    end
    def info(msg) infos << msg end
    def warn(msg) puts msg end
  end

  def random_name
    ('a'..'z').to_a.shuffle[0,6].join
  end

  REDIS_URL = 'redis://localhost:6379/7'
  before :all do
    @bq = Balsamique.new(Redis.connect(url: REDIS_URL))
  end
  before :each do
    @bq.redis.flushdb
    @reporter = Balsamique::Reporter.new(Balsamique.new(
        Redis.connect(url: REDIS_URL)),
      logger: TestLogger.new, poll: 0.2, max_retries: 3)
    @rep_thread = Thread.new { @reporter.run }
  end
  after :each do
    @reporter.stop
    @rep_thread.join
  end

  after :all do
    @bq.redis.flushdb
  end

  let(:tasks) { (1..3).map { |i| [random_name + "_#{i}"] } }
  let(:args) { { 'time' => Time.now.to_i, 'rand' => SecureRandom.base64(6) } }

  it 'reports failed and successful task completions' do
    queues = tasks.map(&:first)
    mtasks = tasks.map(&:dup)
    _, id = @bq.enqueue(tasks, args)
    mtasks.each_with_index do |mtask, i|
      job = @bq.dequeue(queues.shuffle)
      if i.even?
        fail_details = { 't' => Time.now.to_f, 'x' => SecureRandom.base64(6) }
        @bq.fail(id, queues[i], fail_details)
        sleep(0.3)
        fail_report = @reporter.logger.infos.pop
        r_id, r_ts, r_retries, r_status = fail_report.split(' ', 4)
        r_ts = r_ts.to_f
        r_retries = r_retries.to_i
        r_status = JSON.parse(r_status)
        expect(r_id).to eq(id)
        expect(r_retries).to eq(1)
        expect(r_status).to match(
          'task' => queues[i], 'timestamps' => [Float, Float], 'args' => args,
          'tasks' => mtasks,
          'failures' => [{
              'task' => queues[i], 'retries' => 1, 'ts' => Float,
              'details' => fail_details }])
      end
      mtasks[i] << SecureRandom.base64(6)
      @bq.succeed(id, mtasks)
      sleep(0.3)
      infos = @reporter.logger.infos
      expect(infos.size).to eq(i + 1)
      r_id, r_ts, r_retries, r_status = infos.last.split(' ', 4)
      r_ts = r_ts.to_f
      r_retries = r_retries.to_i
      r_status = JSON.parse(r_status)
      expect(r_id).to eq(id)
      expect(r_retries).to eq(1)
        expect(r_status).to match(
          'task' => queues[i + 1], 'timestamps' => [Float], 'args' => args,
          'tasks' => mtasks)
      expect(r_status['timestamps'].first).to be_within(0.0001).of(r_ts)
    end
    expect(@bq.job_status(id)).to eq({})
  end
end
