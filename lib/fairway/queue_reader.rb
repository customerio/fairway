module Fairway
  class QueueReader
    def initialize(connection, *queue_names)
      @connection = connection
      @queue_names = [queue_names].flatten!
    end

    def length
      @connection.redis { |conn| conn.mget(@queue_names.map{|q| "#{q}:length" }).sum.to_i }
    end

    def pull
      @connection.scripts.fairway_pull(@queue_names)
    end
  end
end
