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

    def scripts
      @config.scripts
    end

  end
end
