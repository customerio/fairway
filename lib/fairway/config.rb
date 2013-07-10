module Fairway
  class Config
    attr_accessor :namespace
    attr_reader :defined_queues, :redis_options

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
      @redis ||= pool { Redis::Namespace.new(@namespace, redis: raw_redis) }
    end

    def scripts
      @scripts ||= begin
        Scripts.new(pool { raw_redis }, @namespace)
      end
    end

    private

    def pool(&block)
      pool_size    = @redis_options[:pool]    || 1
      pool_timeout = @redis_options[:timeout] || 5

      ConnectionPool.new(size: pool_size, timeout: pool_timeout) do
        yield
      end
    end

    def raw_redis
      Redis.new(@redis_options)
    end
  end
end
