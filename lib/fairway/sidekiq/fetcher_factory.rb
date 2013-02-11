module Fairway
  module Sidekiq
    class FetcherFactory
      def initialize(fetch)
        @fetch = fetch
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
        @fetcher = Fetcher.new(mgr, @fetch)
      end
    end
  end
end
