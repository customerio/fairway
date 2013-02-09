require "spec_helper"

module Driver
  describe Config do
    it "allows setting of redis connection options" do
      Driver.configure do |config|
        config.redis = { host: "127.0.0.1", port: 6379 }
      end

      Driver.config.redis.should == { host: "127.0.0.1", port: 6379 }
    end

    it "allows setting of redis namespace" do
      Driver.configure do |config|
        config.namespace = "driver:backbone"
      end

      Driver.config.namespace.should == "driver:backbone"
    end
  end
end
