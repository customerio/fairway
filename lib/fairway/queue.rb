module Fairway
  class Queue
    attr_reader :connection, :queue_names

    def initialize(connection, *queue_names)
      @connection  = connection
      @queue_names = parse_queue_names(queue_names)
    end

    def length
      @connection.redis.mget(@queue_names.uniq.map{|q| "#{q}:length" }).sum.to_i
    end

    def pull
      @connection.scripts.fairway_pull(@queue_names.shuffle.uniq)
    end

    def ==(other)
      other.respond_to?(:connection) &&
      other.respond_to?(:queue_names) &&
      connection == other.connection &&
      queue_names == other.queue_names
    end

    private

    def parse_queue_names(names)
      [].tap do |queues|
        names.each do |name|
          if name.is_a?(Hash)
            name.each do |key, value|
              value.times { queues << key }
            end
          else
            queues << name
          end
        end
      end
    end
  end
end
