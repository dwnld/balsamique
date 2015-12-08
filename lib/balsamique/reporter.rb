require_relative '../balsamique'
require 'logger'

class Balsamique
  class Reporter
    def initialize(balsamique, options = {})
      @bq = balsamique
      @poll = options.fetch(:poll, 10.0)
      @max_retries = options.fetch(:max_retries, 20)
      @logger = options.fetch(:logger, Logger.new($stdout))
    end

    attr_accessor :bq, :stopped, :max_retries, :poll, :logger

    def stop
      @stopped = true
    end

    def perform(id, job_status, timestamp, retries)
      logger.info("#{id} #{timestamp} #{retries} #{job_status.to_json}")
    end

    def job_status(id)
      status = bq.job_status(id)
      if status[id]
        bq.fill_job_failures(status)
        bq.fill_args_tasks(status)
      end
      status[id]
    end

    def run
      @stopped = false
      until stopped do
        report = bq.pop_report(Time.now.to_f)
        if report
          id, timestamp, retries = report
          begin
            if (status = job_status(id))
              perform(id, status, timestamp, retries)
              bq.remove_job(id) unless status[:task]
            else
              logger.info("#{id} #{timestamp} #{retries} null")
            end
            bq.complete_report(id)
          rescue => error
            logger.warn(
              "#{id} #{timestamp} #{retries} " +
              "#{error.class.name} #{error.message}")
            logger.debug(error)
            bq.complete_report(id) if retries > max_retries
          end
        else
          t_poll = Time.now.to_f + poll * (1.0 - 0.5 * rand())
          sleep 0.1 until Time.now.to_f > t_poll || stopped
        end
      end
    end
  end
end
