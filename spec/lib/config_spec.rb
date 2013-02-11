require "spec_helper"

module Driver
  describe Config do
    describe "#initialize" do
      it "yields itself to a block" do
        config = Config.new do |c|
          c.namespace = "x"
        end
        config.namespace.should == "x"
      end
    end

    describe "#facet" do
      context "when called with a block" do
        it "sets the facet" do
          config = Config.new
          config.facet do |message|
            "foo"
          end
          config.facet.call({}).should == "foo"
        end
      end
    end

    describe "#register_queue" do
      it "requires a name"
      it "accepts a channel"
    end

    it "allows setting of redis connection options" do
      config = Config.new do |config|
        config.redis = { host: "127.0.0.1", port: 6379 }
      end

      config.redis.should == { host: "127.0.0.1", port: 6379 }
    end

    it "allows setting of redis namespace" do
      config = Config.new do |config|
        config.namespace = "driver:backbone"
      end

      config.namespace.should == "driver:backbone"
    end

    it "sets the default facet" do
      config = Config.new
      config.facet_for(environment_id: 5, facet: 1).should == 1
    end

    it "allows custom faceting" do
      config = Config.new do |config|
        config.facet do |message|
          message[:environment_id]
        end
      end

      config.facet_for(environment_id: 5, facet: 1).should == 5
    end

    it "sets the default topic" do
      config = Config.new
      config.topic_for(id: 5, topic: "message").should == "message"
    end

    it "allows custom topics" do
      config = Config.new do |config|
        config.topic do |message|
          message[:class]
        end
      end

      config.topic_for(class: "SomeClass", topic: "message").should == "SomeClass"
    end
  end
end
