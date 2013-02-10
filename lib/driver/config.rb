module Driver
  class Config
    attr_accessor :redis, :namespace

    def initialize
      @redis = {}
      @namespace = nil
      @facet = lambda { |message| message[:facet] }
      @topic = lambda { |message| message[:topic] }
    end

    def facet_for(message)
      @facet.call(message)
    end

    def facet(&block)
      @facet = block
    end

    def topic_for(message)
      @topic.call(message)
    end

    def topic(&block)
      @topic = block
    end
  end
end
