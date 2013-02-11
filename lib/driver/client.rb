require "driver/scripts"

module Driver
  class Client
    def deliver(message)
      scripts.driver_deliver(
        namespace,
        Driver.config.topic_for(message),
        Driver.config.facet_for(message),
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
      @redis ||= Redis::Namespace.new(Driver.config.namespace, redis: raw_redis)
    end

    private

    def namespace
      if Driver.config.namespace.blank?
        ""
      else
        "#{Driver.config.namespace}:"
      end
    end

    def scripts
      @scripts ||= Scripts.new(raw_redis)
    end

    def raw_redis
      @raw_redis ||= Redis.new(Driver.config.redis.merge(hiredis: true))
    end
  end
end
