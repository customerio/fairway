require "fairway/scripts"

module Fairway
  class Connection
    DEFAULT_CHANNEL = "default"

    def initialize(config = Fairway.config)
      @config = config

      @config.defined_queues.each do |queue|
        scripts.register_queue(queue.name, queue.channel)
      end
    end

    def queues
      @queues ||= begin
        scripts.registered_queues.map do |name, _|
          Queue.new(self, name)
        end
      end
    end

    def deliver(message, channel = DEFAULT_CHANNEL)
      scripts.fairway_deliver(
        channel,
        @config.facet.call(message),
        message.to_json
      )
    end

    def subscribe(channel_pattern, &block)
      redis.psubscribe(channel_pattern) do |on|
        on.pmessage do |pattern, channel, message|
          block.call(channel, message)
        end
      end
    end

    def scripts
      @config.scripts
    end

    def redis
      @config.redis
    end
  end
end
