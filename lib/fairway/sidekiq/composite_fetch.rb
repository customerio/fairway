module Fairway
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
        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work"

        fetch_order.each do |fetch|
          work = fetch.retrieve_work(blocking: false)

          if work
            ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got work"
            return work
          end
        end

        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got nil"
        sleep 1

        return nil
      end
    end
  end
end
