module Fairway
  class RandomDistribution
    class CannotConnect < RuntimeError; end

    EXCEPTIONS = [
      Redis::CannotConnectError,
      Errno::ETIMEDOUT,
      Errno::EHOSTUNREACH
    ]

    attr_reader :pools

    def initialize(pools)
      @pools = pools
    end

    def with(&block)
      valid_pools = @pools

      while valid_pools.any?
        pool = valid_pools.sample

        pool.with do |conn|
          begin
            return yield(conn)
          rescue *EXCEPTIONS => e
            puts "FAIRWAY WITH EXCEPTION: #{e}"
            valid_pools -= [pool]
          end
        end
      end

      raise CannotConnect.new
    end

    def with_each_running(&block)
      @pools.shuffle.each do |pool|
        pool.with do |conn|
          begin
            yield(conn)
          rescue *EXCEPTIONS => e
            puts "FAIRWAY WITH EACH RUNNING EXCEPTION: #{e}"
          end
        end
      end
    end

    def with_each(&block)
      @pools.shuffle.each do |pool|
        pool.with do |conn|
          yield(conn)
        end
      end
    end
  end

  class Config
    attr_accessor :namespace
    attr_reader :defined_queues, :redis_options, :distribute

    DEFAULT_FACET = "default"

    QueueDefinition = Struct.new(:name, :channel)

    def initialize
      @redis_options  = []
      @namespace      = nil
      @distribute     = RandomDistribution
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

        @distribute.new(pools)
      end
    end

    def raw_redises
      @raw_redises ||= begin
        @redis_options << {} if @redis_options.empty?
        pools = @redis_options.map do |options|
          pool(options) { raw_redis(options) }
        end

        @distribute.new(pools)
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
