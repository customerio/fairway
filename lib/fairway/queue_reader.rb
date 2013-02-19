module Fairway
  class QueueReader
    attr_reader :connection, :queue_names

    def initialize(connection, *queue_names)
      @connection  = connection
      @queue_names = [queue_names].flatten!
    end

    def length
      @connection.redis.mget(@queue_names.map{|q| "#{q}:length" }).sum.to_i
    end

    def pull
      @connection.scripts.fairway_pull(@queue_names)
    end

    def ==(other)
      connection == other.connection && queue_names == other.queue_names
    end
  end
end
