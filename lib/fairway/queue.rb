module Fairway
  class Queue
    attr_reader :connection, :queue_names

    def initialize(connection, *queue_names)
      @connection  = connection
      @queue_names = parse_queue_names(queue_names)
    end

    def active_facets
      each_queue do |queue|
        redis.smembers("#{queue}:active_facets")
      end.flatten.uniq
    end

    def length
      redis.mget(unique_queues.map{|q| "#{q}:length" }).sum.to_i
    end

    def facet_length(facet)
      each_queue do |queue|
        redis.llen("#{queue}:#{facet}")
      end.sum
    end

    def peek
      scripts.fairway_peek(@queue_names.shuffle.uniq)
    end

    def pull
      scripts.fairway_pull(@queue_names.shuffle.uniq)
    end

    def ==(other)
      other.respond_to?(:connection) &&
      other.respond_to?(:queue_names) &&
      connection == other.connection &&
      queue_names == other.queue_names
    end

    private

    def unique_queues
      @queue_names.uniq
    end

    def each_queue(&block)
      unique_queues.map do |queue|
        yield(queue)
      end
    end

    def scripts
      @connection.scripts
    end

    def redis
      @connection.redis
    end

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
