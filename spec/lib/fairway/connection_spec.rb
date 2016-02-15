require "spec_helper"

module Fairway
  describe Connection do
    let(:connection) { Connection.new(Fairway.config) }
    let(:redis)      { Fairway.config.redis }
    let(:message)    { { facet: 1, topic: "event:helloworld" } }

    describe "#initialize" do
      it "registers queues from the config" do
        redis.with do |conn|
          Fairway.config.register_queue("myqueue", ".*")
          conn.hgetall("registered_queues").should == {}

          Connection.new(Fairway.config)

          conn.hgetall("registered_queues").should == {
            "myqueue" => ".*"
          }
        end
      end

      context "when an existing queue definition does not match" do
        it "raises a QueueMismatchError"
      end
    end

    describe "#queues" do
      it "returns a Queue for every currently registered queue" do
        redis.with do |conn|
          conn.hset("registered_queues", "name", "channel")
        end

        connection.queues.should == [
          Queue.new(connection, "name")
        ]
      end
    end

    describe "#deliver" do
      it "publishes message over the message topic channel" do
        redis = Redis.new

        redis.psubscribe("*") do |on|
          on.psubscribe do |pattern, total|
            connection.deliver(message)
          end

          on.pmessage do |pattern, channel, received_message|
            received_message.should == message.to_json
            channel.should == "test:fairway:default"
            redis.punsubscribe(pattern)
          end
        end
      end

      context "registered queue exists for message type" do
        before do
          Fairway.config.register_queue("myqueue")
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
            conn.hset("myqueue:facet_pool", "1", "1")
            connection.deliver(message)
            conn.llen("myqueue:facet_queue").should == 0
          end
        end
      end

      context "unregistering a queue" do
        before do
          Fairway.config.register_queue("myqueue")
        end

        it "stops adding messages to the queue" do
          connection.deliver(message)
          connection.unregister_queue("myqueue")
          connection.deliver(message)


          redis.with do |conn|
            conn.llen("myqueue:1").should == 1
          end
        end
      end
    end
  end
end
