module Driver
  class ChanneledConnection
    def initialize(connection, &block)
      @connection = connection
      @block = block
    end

    def deliver(message)
      channel = @block.call(message)
      @connection.deliver(message, channel)
    end

    def scripts
      @connection.scripts
    end
  end
end
