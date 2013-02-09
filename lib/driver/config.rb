module Driver
  class Config
    attr_accessor :redis, :namespace

    def initialize
      self.redis = {}
      self.namespace = nil
    end
  end
end
