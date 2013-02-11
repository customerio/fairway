module Driver
  module Sidekiq
    class CompositeFetch
      def initialize(fetches)
        @fetches = []

        fetches.each do |fetch, weight|
          [weight.to_i, 1].max.times do
            @fetches << fetch
          end
        end
      end

      def retrieve_work
        @queues.shuffle.uniq.detect do |fetch|
          fetch.retrieve_work
        end
      end
    end
  end
end
