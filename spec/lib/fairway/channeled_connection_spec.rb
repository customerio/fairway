require "spec_helper"

module Fairway
  describe ChanneledConnection do
    let(:config) do
      Config.new do |c|
        c.facet { |message| message[:facet] }
      end
    end
    let(:base_connection) { Connection.new(config) }
    let(:connection) do
      ChanneledConnection.new(base_connection) do |message|
        message[:topic]
      end
    end
    let(:redis)   { config.redis }
    let(:message) { { facet: 1, topic: "event:helloworld" } }

    it "delegates non existant methods to parent connection" do
      base_connection.should_receive(:random_method) do |arg1, arg2, &block|
        block.call(arg1, arg2)
      end

      connection.random_method(1, 2) do |arg1, arg2|
        arg1.should == 1
        arg2.should == 2
      end
    end

    describe "#deliver" do
      context "multiple queues exist for message type" do
        it "adds message for both queues" do
          config.register_queue("myqueue", ".*:helloworld")
          config.register_queue("yourqueue", "event:.*world")
          connection.deliver(message)

          redis.with do |conn|
            conn.llen("myqueue:1").should == 1
            conn.llen("yourqueue:1").should == 1
          end
        end
      end

      context "registered queue exists for message type" do
        before do
          config.register_queue("myqueue", "event:helloworld")
        end

        it "adds message to the environment facet for the queue" do
          connection.deliver(message)

          redis.with do |conn|
            conn.llen("myqueue:1").should == 1
            conn.lindex("myqueue:1", 0).should == message.to_json
          end
        end

        it "adds facet to list of active facets" do
          connection.deliver(message)

          redis.with do |conn|
            conn.smembers("myqueue:active_facets").should == ["1"]
          end
        end

        it "pushes facet onto facet queue" do
          connection.deliver(message)

          redis.with do |conn|
            conn.llen("myqueue:facet_queue").should == 1
            conn.lindex("myqueue:facet_queue", 0).should == "1"
          end
        end

        it "doesn't push onto to facet queue if currently active" do
          redis.with do |conn|
            conn.sadd("myqueue:active_facets", "1")
            connection.deliver(message)
            conn.llen("myqueue:facet_queue").should == 0
          end
        end
      end

      context "registered queue exists for another message type" do
        before do
          config.register_queue("myqueue", "foo")
        end

        it "doesn't add message to the queue" do
          connection.deliver(message)

          redis.with do |conn|
            conn.llen("myqueue:1").should == 0
          end
        end

        it "doesn't add facet to list of active facets" do
          connection.deliver(message)
          
          redis.with do |conn|
            conn.smembers("myqueue:active_facets").should == []
          end
        end
      end
    end
  end
end
