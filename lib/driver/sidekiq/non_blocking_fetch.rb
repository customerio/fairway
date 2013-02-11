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

          conn.eval(script, queues_cmd.map{|q| "#{namespace}#{q}"})
        end

        if (work)
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got work"
          work = UnitOfWork.new(*work)
        else
          ::Sidekiq.logger.debug "#{self.class.name}#retrieve_work got nil"
        end

        work
      end

      def queues_cmd
        @queues.shuffle.uniq
      end

      def namespace
        @namespace ||= begin
          namespaces = []

          ::Sidekiq.redis do |conn|
            wrapper = conn

            while defined?(wrapper.redis)
              namespaces.unshift(wrapper.namespace)
              wrapper = wrapper.redis
            end

            namespace = namespaces.join(":")
            namespace += ":" unless namespace.blank?
            namespace
          end
        end
      end
    end
  end
end
