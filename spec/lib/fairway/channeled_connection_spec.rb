require "spec_helper"

module Fairway
  describe ChanneledConnection do
    let(:config) do
      Config.new do |c|
        c.facet { |message| message[:facet] }
      end
    end
    let(:connection) do
      ChanneledConnection.new(Connection.new(config)) do |message|
        message[:topic]
      end
    end
    let(:redis)  { config.redis }
    let(:message) { { facet: 1, topic: "event:helloworld" } }

    describe "#deliver" do
      context "multiple queues exist for message type" do
        it "adds message for both queues" do
          config.register_queue("myqueue", ".*:helloworld")
          config.register_queue("yourqueue", "event:.*world")
          connection.deliver(message)
          redis.llen("myqueue:1").should == 1
          redis.llen("yourqueue:1").should == 1
        end
      end

      context "registered queue exists for message type" do
        before do
          config.register_queue("myqueue", "event:helloworld")
        end

        it "adds message to the environment facet for the queue" do
          connection.deliver(message)
          redis.llen("myqueue:1").should == 1
          redis.lindex("myqueue:1", 0).should == message.to_json
        end

        it "adds facet to list of active facets" do
          connection.deliver(message)
          redis.smembers("myqueue:active_facets").should == ["1"]
        end

        it "pushes facet onto facet queue" do
          connection.deliver(message)
          redis.llen("myqueue:facet_queue").should == 1
          redis.lindex("myqueue:facet_queue", 0).should == "1"
        end

        it "doesn't push onto to facet queue if currently active" do
          redis.sadd("myqueue:active_facets", "1")
          connection.deliver(message)
          redis.llen("myqueue:facet_queue").should == 0
        end
      end

      context "registered queue exists for another message type" do
        before do
          config.register_queue("myqueue", "foo")
        end

        it "doesn't add message to the queue" do
          connection.deliver(message)
          redis.llen("myqueue:1").should == 0
        end

        it "doesn't add facet to list of active facets" do
          connection.deliver(message)
          redis.smembers("myqueue:active_facets").should == []
        end
      end
    end
  end
end
