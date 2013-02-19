require "fairway/scripts"

module Fairway
  class Connection
    DEFAULT_CHANNEL = "default"

    def initialize(config = Fairway.config)
      @config = config

      @config.queues.each do |queue|
        scripts.fairway_register_queue(queue.name, queue.channel)
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
      redis do |conn|
        conn.psubscribe(channel_pattern) do |on|
          on.pmessage do |pattern, channel, message|
            block.call(channel, message)
          end
        end
      end
    end

    def scripts
      @config.scripts
    end

    def redis(&block)
      @config.redis(&block)
    end
  end
end
