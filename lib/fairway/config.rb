module Fairway
  class RandomPool
    attr_reader :pools

    def initialize(pools)
      @pools = pools
    end

    def with(&block)
      @pools.sample.with do |conn|
        yield(conn)
      end
    end
  end

  class Config
    attr_accessor :namespace
    attr_reader :defined_queues, :redis_options

    DEFAULT_FACET = "default"

    QueueDefinition = Struct.new(:name, :channel)

    def initialize
      @redis_options  = []
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
      @redis_options = [options].flatten
    end

    def redis
      @redis ||= redises
    end

    def scripts
      @scripts ||= begin
        Scripts.new(raw_redises, @namespace)
      end
    end

    private

    def redises
      @redises ||= begin
        @redis_options << {} if @redis_options.empty?
        pools = @redis_options.map do |options|
          pool(options) { Redis::Namespace.new(@namespace, redis: raw_redis(options)) }
        end

        RandomPool.new(pools)
      end
    end

    def raw_redises
      @raw_redises ||= begin
        @redis_options << {} if @redis_options.empty?
        pools = @redis_options.map do |options|
          pool(options) { raw_redis(options) }
        end

        RandomPool.new(pools)
      end
    end

    def pool(options, &block)
      pool_size    = options[:pool]    || 1
      pool_timeout = options[:timeout] || 5

      ConnectionPool.new(size: pool_size, timeout: pool_timeout) do
        yield
      end
    end

    def raw_redis(options)
      Redis.new(options)
    end
  end
end
