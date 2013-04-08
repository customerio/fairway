module Fairway
  module Sidekiq
    class Fetch < ::Sidekiq::BasicFetch
      class Fetches
        attr_reader :list

        def from(queue, weight = 1, &block)
          if queue == :sidekiq
            queue = BasicFetch.new(::Sidekiq.options)
          else
            queue = FairwayFetch.new(queue, &block)
          end

          weight.times do
            list << queue
          end
        end

        def list
          @list ||= []
        end
      end

      def initialize(&block)
        yield(@fetches = Fetches.new)
      end

      def new(options)
        self
      end

      def fetches
        @fetches.list
      end

      def fetch_order
        fetches.shuffle.uniq
      end

      def retrieve_work(options = {})
        options = { blocking: true }.merge(options)

        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work"

        fetch_order.each do |fetch|
          work = fetch.retrieve_work(blocking: false)

          if work
            ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got work"
            return work
          end
        end

        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got nil"
        sleep 1 if options[:blocking]

        return nil
      end
    end
  end
end
