module Driver
  module Sidekiq
    class NonBlockingFetch < ::Sidekiq::BasicFetch
      def initialize(queues)
        queues.each do |queue, weight|
          [weight.to_i, 1].max.times do
            @queues << "queue:#{queue}"
          end
        end
      end

      def queues_cmd
        queues = @queues.shuffle.uniq
        queues << 0 # return immediately if nothing on queue
      end
    end
  end
end
