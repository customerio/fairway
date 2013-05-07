module Fairway
  class Queue
    attr_reader :connection, :queue_names

    def initialize(connection, *queue_names)
      @connection  = connection
      @queue_names = parse_queue_names(queue_names)
    end

    def active_facets
      facet_names = unique_queues.map do |queue|
        redis.smembers("#{queue}:active_facets")
      end.flatten.uniq
      
      facet_names.map do |name|
        Facet.new(self, name)
      end
    end

    def length
      redis.mget(unique_queues.map{|q| "#{q}:length" }).sum.to_i
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

    def unique_queues
      @queue_names.uniq
    end

    def queue_key
      queue
    end

    def redis
      @connection.redis
    end

    def scripts
      @connection.scripts
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
