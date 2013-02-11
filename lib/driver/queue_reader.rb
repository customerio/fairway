module Driver
  class QueueReader
    def initialize(connection, *queue_names)
      @connection = connection
      @queue_names = [queue_names].flatten!
    end

    def pull
      @connection.scripts.driver_pull(@queue_names)
    end

  end
end
