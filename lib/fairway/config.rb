module Fairway
  class Config
    attr_accessor :namespace
    attr_reader :queues

    DEFAULT_FACET = "default"

    QueueDefinition = Struct.new(:name, :channel)

    def initialize
      @redis_options = {}
      @namespace = nil
      @facet = lambda { |message| DEFAULT_FACET }
      @queues = []
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
      @queues << QueueDefinition.new(name, channel)
    end

    def redis=(options)
      @redis_options = options
    end

    def redis(&block)
      raw_redis.with do |conn|
        yield Redis::Namespace.new(@namespace, redis: conn)
      end
    end

    def scripts
      @scripts ||= Scripts.new(raw_redis, scripts_namespace)
    end

  private

    def scripts_namespace
      if @namespace.blank?
        ""
      else
        "#{@namespace}:"
      end
    end

    def raw_redis
      @raw_redis ||= begin
        size    = @redis_options.delete(:size) || 1
        timeout = @redis_options.delete(:timeout) || 1
        ConnectionPool.new(size: size, timeout: timeout) { Redis.new(@redis_options) }
      end
    end
  end
end
