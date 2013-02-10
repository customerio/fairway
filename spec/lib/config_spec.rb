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
    
    it "sets the default facet" do
      Driver.config.facet_for(environment_id: 5, facet: 1).should == 1
    end

    it "allows custom faceting" do
      Driver.configure do |config|
        config.facet do |message|
          message[:environment_id]
        end
      end

      Driver.config.facet_for(environment_id: 5, facet: 1).should == 5
    end

    it "sets the default topic" do
      Driver.config.topic_for(id: 5, topic: "message").should == "message"
    end

    it "allows custom topics" do
      Driver.configure do |config|
        config.topic do |message|
          message[:class]
        end
      end

      Driver.config.topic_for(class: "SomeClass", topic: "message").should == "SomeClass"
    end
  end
end
