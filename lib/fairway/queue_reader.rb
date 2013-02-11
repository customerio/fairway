module Fairway
  class QueueReader
    def initialize(connection, *queue_names)
      @connection = connection
      @queue_names = [queue_names].flatten!
    end

    def pull
      @connection.scripts.fairway_pull(@queue_names)
    end
  end
end
