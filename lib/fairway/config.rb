module Fairway
  class Config
    attr_accessor :namespace
    attr_reader :defined_queues

    DEFAULT_FACET = "default"

    QueueDefinition = Struct.new(:name, :channel)

    def initialize
      @redis_options  = {}
      @namespace      = nil
      @facet          = lambda { |message| DEFAULT_FACET }
      @defined_queues = []

      yield self if block_given?
    end

    def facet(&block)
      if block_given?
        @facet = block
      else
        @facet
      end
    end

    def register_queue(name, channel = Connection::DEFAULT_CHANNEL)
      @defined_queues << QueueDefinition.new(name, channel)
    end

    def redis=(options)
      @redis_options = options
    end

    def redis
      @redis ||= Redis::Namespace.new(@namespace, redis: raw_redis)
    end

    def scripts
      @scripts ||= Scripts.new(raw_redis, @namespace)
    end

  private

    def raw_redis
      @raw_redis ||= Redis.new(@redis_options.merge(hiredis: true))
    end

  end
end
