module Fairway
  class Facet
    class InvalidPriorityError < Exception; end

    attr_reader :queue, :name

    def initialize(queue, facet_name)
      @queue = queue
      @name  = facet_name
    end

    def length
      redis.with do |conn|
        each_queue do |queue|
          conn.llen(facet_key(queue))
        end.sum
      end
    end

    def priority
      redis.with do |conn|
        each_queue do |queue|
          (conn.hget(priority_key(queue), name) || 1).to_i
        end
      end
    end

    def priority=(priority)
      validate_priority!(priority)

      each_queue do |queue|
        scripts.fairway_priority(queue, name, priority)
      end
    end

    def facet_key(queue)
      "#{queue}:#{name}"
    end

    def priority_key(queue)
      "#{queue}:priorities"
    end

    def ==(other)
      other.respond_to?(:queue) &&
      other.respond_to?(:name) &&
      queue == other.queue &&
      name == other.name
    end

    private

    def redis
      queue.redis
    end

    def scripts
      queue.scripts
    end

    def each_queue(&block)
      queue.unique_queues.map do |queue|
        yield(queue)
      end
    end

    def validate_priority!(priority)
      if priority.to_i.to_s != priority.to_s || priority.to_i < 0
        raise InvalidPriorityError.new("Facet priority must be an integer >= 0")
      end
    end
  end
end
