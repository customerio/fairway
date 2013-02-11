module Driver
  module Sidekiq
    class CompositeFetch
      attr_reader :fetches

      def initialize(fetches)
        @fetches = []

        fetches.each do |fetch, weight|
          [weight.to_i, 1].max.times do
            @fetches << fetch
          end
        end
      end

      def fetch_order
        fetches.shuffle.uniq
      end

      def retrieve_work
        fetch_order.detect do |fetch|
          work = fetch.retrieve_work
          return work if work
        end
      end
    end
  end
end
