require "spec_helper"

module Fairway
  describe Scripts do
    describe "#initialize" do
      it "requires a redis client" do
        lambda {
          Scripts.new
        }.should raise_error(ArgumentError)
      end
    end

    describe "#register_queue" do
      let(:scripts) { Scripts.new(Redis.new, "foo") }

      it "adds the queue and channel to the hash of registered queues" do
        scripts.register_queue("name", "channel")
        Redis.new.hgetall("foo:registered_queues").should == { "name" => "channel" }
      end
    end

    describe "#registered_queue" do
      let(:scripts) { Scripts.new(Redis.new, "foo") }

      it "returns hash of all registered queues and their channels" do
        Redis.new.hset("foo:registered_queues", "first", "channel1")
        Redis.new.hset("foo:registered_queues", "second", "channel2")
        scripts.registered_queues.should == { "first" => "channel1", "second" => "channel2" }
      end
    end

    describe "#method_missing" do
      let(:scripts) { Scripts.new(Redis.new, "foo") }

      it "runs the script" do
        scripts.fairway_pull("namespace", "name")
      end

      context "when the script does not exist" do
        it "loads the script" do
          Redis.new.script(:flush)
          scripts.fairway_pull("namespace", "name")
        end
      end
    end
  end
end
