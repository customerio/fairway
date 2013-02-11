module Driver
  class Config
    attr_accessor :namespace
    attr_reader :queues

    DEFAULT_FACET = "default"

    QueueDefinition = Struct.new(:name, :topic)

    def initialize
      @redis_options = {}
      @namespace = nil
      @facet = lambda { |message| DEFAULT_FACET }
      @topic = lambda { |message| message[:topic] }
      @queues = []
      yield self if block_given?
    end

    def facet_for(message)
      @facet.call(message)
    end

    def facet(&block)
      if block_given?
        @facet = block
      else
        @facet
      end
    end

    def register_queue(name, topic)
      @queues << QueueDefinition.new(name, topic)
    end

    def topic_for(message)
      @topic.call(message)
    end

    def topic(&block)
      @topic = block
    end

    def redis=(options)
      @redis_options = options
    end

    def redis
      @redis ||= Redis::Namespace.new(@namespace, redis: raw_redis)
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
      @raw_redis ||= Redis.new(@redis_options.merge(hiredis: true))
    end

  end
end
