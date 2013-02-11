require "sidekiq/fetch"

module Driver
  module Sidekiq
    class QueueFetch < ::Sidekiq::BasicFetch
      def initialize(queue_reader, &block)
        @queue_reader = queue_reader
        @message_to_job = block if block_given?
      end

      def retrieve_work
        if work = @queue_reader.pull
          work = @message_to_job.call(work) if @message_to_job
          UnitOfWork.new(work["queue"], work)
        end
      end
    end
  end
end
