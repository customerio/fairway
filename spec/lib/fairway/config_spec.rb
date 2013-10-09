require "spec_helper"

module Fairway
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

    it "allows redis config" do
      Config.new do |config|
        config.redis = { host: "127.0.0.1", port: 6379 }
      end
    end

    it "allows multiple redis config" do
      Config.new do |config|
        config.redis = [
          { host: "127.0.0.1", port: 6379 },
          { host: "127.0.0.1", port: 6380 }
        ]

        config.redis.pools.length.should == 2
      end
    end

    it "allows setting of connection pooling" do
      config = Config.new do |config|
        config.redis = { pool: 10 }
      end

      config.redis.pools.first.instance_variable_get("@size").should == 10
    end

    it "defaults to pool of 1" do
      config = Config.new do |config|
      end

      config.redis.pools.first.instance_variable_get("@size").should == 1
    end

    it "allows setting of redis namespace" do
      config = Config.new do |config|
        config.namespace = "ns"
      end

      config.namespace.should == "ns"
    end

    it "sets the default facet" do
      config = Config.new
      config.facet.call(environment_id: 5, facet: 1).should == Config::DEFAULT_FACET
    end

    it "allows custom faceting" do
      config = Config.new do |config|
        config.facet do |message|
          message[:environment_id]
        end
      end

      config.facet.call(environment_id: 5, facet: 1).should == 5
    end
  end
end
