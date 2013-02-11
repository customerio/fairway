require "driver/scripts"

module Driver
  class Connection
    def initialize(config = Driver.config)
      @config = config

      @config.queues.each do |queue|
        scripts.driver_register_queue(queue.name, queue.topic)
      end
    end

    def deliver(message)
      scripts.driver_deliver(
        @config.topic_for(message),
        @config.facet.call(message),
        message.to_json
      )
    end

    def pull(queues)
      scripts.driver_pull([queues].flatten)
    end

  private

    def scripts
      @config.scripts
    end

  end
end
