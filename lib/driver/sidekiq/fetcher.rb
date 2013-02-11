module Driver
  module Sidekiq
    class Fetcher < ::Sidekiq::Fetcher
      def initialize(mgr, fetch)
        @mgr = mgr
        @strategy = fetch
      end

      def done!
        @done = true
      end

      def fetch
        watchdog('Fetcher#fetch died') do
          return if @done

          begin
            work = @strategy.retrieve_work

            if work
              @mgr.async.assign(work)
            else
              sleep(TIMEOUT) # Sleep before returning to work
              after(0) { fetch }
            end
          rescue => ex
            logger.error("Error fetching message: #{ex}")
            logger.error(ex.backtrace.first)
            sleep(TIMEOUT)
            after(0) { fetch }
          end
        end
      end
    end
  end
end
