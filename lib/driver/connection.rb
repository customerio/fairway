require "driver/scripts"

module Driver
  class Connection
    DEFAULT_CHANNEL = "default"

    def initialize(config = Driver.config)
      @config = config

      @config.queues.each do |queue|
        scripts.driver_register_queue(queue.name, queue.topic)
      end
    end

    def deliver(message, channel = DEFAULT_CHANNEL)
      scripts.driver_deliver(
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
