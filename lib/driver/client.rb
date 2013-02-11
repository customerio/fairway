require "driver/scripts"

module Driver
  class Client
    def initialize(config = Driver.config)
      @config = config
    end

    def deliver(message)
      scripts.driver_deliver(
        namespace,
        @config.topic_for(message),
        @config.facet_for(message),
        message.to_json
      )
    end

    def register_queue(name, topic)
      scripts.driver_register_queue(namespace, name, topic)
    end

    def pull(queues)
      scripts.driver_pull(namespace, [queues].flatten)
    end

    def redis
      @redis ||= Redis::Namespace.new(@config.namespace, redis: raw_redis)
    end

    private

    def namespace
      if @config.namespace.blank?
        ""
      else
        "#{@config.namespace}:"
      end
    end

    def scripts
      @scripts ||= Scripts.new(raw_redis)
    end

    def raw_redis
      @raw_redis ||= Redis.new(@config.redis.merge(hiredis: true))
    end
  end
end
