module Fairway
  module Sidekiq
    class BasicFetch < ::Sidekiq::BasicFetch
      attr_reader :queues

      def initialize(options)
        @queues = options[:queues].map { |q| "queue:#{q}" }
      end

      def retrieve_work(options = {})
        options = { blocking: true }.merge(options)

        ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work #{queues_cmd}"

        work = ::Sidekiq.redis do |conn|
          script = <<-SCRIPT
            -- take advantage of non-blocking scripts
            for i = 1, #KEYS do
              local work = redis.call('rpop', KEYS[i]);

              if work then
                return {KEYS[i], work};
              end
            end

            return nil;
          SCRIPT

          conn.eval(script, queues_cmd)
        end

        if (work)
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got work"
          work = UnitOfWork.new(*work)
        else
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got nil"
          sleep 1 if options[:blocking]
        end

        work
      end

      def queues_cmd
        @queues.shuffle.uniq
      end
    end
  end
end
