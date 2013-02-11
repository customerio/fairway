require "sidekiq/fetch"

module Driver
  module Sidekiq
    class QueueFetch < ::Sidekiq::BasicFetch
      def initialize(queue_reader, &block)
        @queue_reader = queue_reader
        @message_to_job = block if block_given?
      end

      def retrieve_work
        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work"
        unit_of_work = nil

        if work = @queue_reader.pull
          work = @message_to_job.call(work) if @message_to_job
          unit_of_work = UnitOfWork.new(work["queue"], work)
        end

        if unit_of_work
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got work"
        else
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got nil"
        end

        unit_of_work
      end
    end
  end
end
