require "spec_helper"

module Fairway
  describe Scripts do
    let(:redis) { Fairway::Config.new.redis }

    describe "#initialize" do
      it "requires a redis client" do
        lambda {
          Scripts.new
        }.should raise_error(ArgumentError)
      end
    end

    describe "#register_queue" do
      let(:scripts) { Scripts.new(redis, "foo") }

      it "adds the queue and channel to the hash of registered queues" do
        scripts.register_queue("name", "channel")

        redis.with do |conn|
          conn.hgetall("foo:registered_queues").should == { "name" => "channel" }
        end
      end
    end

    describe "#unregister_queue" do
      let(:scripts) { Scripts.new(redis, "foo") }

      it "removes the queue and channel from the hash of registered queues" do
        scripts.register_queue("name", "channel")
        scripts.unregister_queue("name")

        redis.with do |conn|
          conn.hgetall("foo:registered_queues").should == {}
        end
      end
    end

    describe "#registered_queues" do
      let(:scripts) { Scripts.new(redis, "foo") }

      it "returns hash of all registered queues and their channels" do
        redis.with do |conn|
          conn.hset("foo:registered_queues", "first", "channel1")
          conn.hset("foo:registered_queues", "second", "channel2")
        end

        scripts.registered_queues.should == { "first" => "channel1", "second" => "channel2" }
      end
    end

    describe "#method_missing" do
      let(:scripts) { Scripts.new(redis, "foo") }

      it "runs the script" do
        scripts.fairway_pull(Time.now.to_i, -1, "name")
      end

      context "when the script does not exist" do
        it "loads the script" do
          redis.with do |conn|
            conn.script(:flush)
          end

          scripts.fairway_pull(Time.now.to_i, -1, "name")
        end
      end
    end
  end
end
