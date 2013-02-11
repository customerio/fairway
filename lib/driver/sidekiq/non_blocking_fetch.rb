module Driver
  module Sidekiq
    class NonBlockingFetch < ::Sidekiq::BasicFetch
      def initialize(queues)
        @queues = []

        queues.each do |queue, weight|
          [weight.to_i, 1].max.times do
            @queues << "queue:#{queue}"
          end
        end
      end

      def retrieve_work
        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work"

        if (work = super)
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got work"
        else
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got nil"
        end

        work
      end

      def queues_cmd
        queues = @queues.shuffle.uniq
        queues << 0 # return immediately if nothing on queue
      end
    end
  end
end
