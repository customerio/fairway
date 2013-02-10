module Driver
  class Client
    def deliver(message)
      raw_redis.eval(DELIVER_MESSAGE, [namespace], [message[:environment_id], message[:type], message[:name], message.to_json])
    end

    def register_queue(name, topic)
      raw_redis.eval(REGISTER_QUEUE, [namespace], [name, topic])
    end

    def pull(queues)
      queues = [queues].flatten
      raw_redis.eval(PULL_QUEUE, [namespace], queues)
    end

    def redis
      @redis ||= begin
         Redis::Namespace.new(Driver.config.namespace, redis: raw_redis)
      end
    end

    private

    def namespace
      if Driver.config.namespace.blank?
        ""
      else
        "#{Driver.config.namespace}:"
      end
    end

    def raw_redis
      @raw_redis ||= Redis.new(Driver.config.redis.merge(hiredis: true))
    end
  end
end
