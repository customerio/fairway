module Fairway
  module Sidekiq
    class FetcherFactory
      def initialize(queue_reader, sidekiq_queues)
        @queue_reader = queue_reader
        @sidekiq_queues = sidekiq_queues
      end

      def done!
        @fetcher.done!
      end

      def strategy
        # This is only used for ::Sidekiq::BasicFetch.bulk_requeue
        # which is the same for us.
        ::Sidekiq::BasicFetch
      end

      def new(mgr, options)
        queue_fetch = QueueFetch.new(@queue_reader)
        non_blocking_fetch = NonBlockingFetch.new(@sidekiq_queues)
        fetch = CompositeFetch.new(queue_fetch => 10, non_blocking_fetch => 1)
        @fetcher = Fetcher.new(mgr, fetch)
      end
    end
  end
end
