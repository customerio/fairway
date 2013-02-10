require "spec_helper"

module Driver
  describe Client do
    let(:client) { Client.new }
    let(:redis)  { client.redis }

    let(:message) { { environment_id: 1, type: "event", name: "helloworld" } }


    it "uses the driver's config to build a redis connection" do
      Driver.configure do |config|
        config.redis     = { host: "127.0.0.1", port: 6379 }
        config.namespace = "driver:backbone"
      end

      redis.redis.client.host.should == "127.0.0.1"
      redis.redis.client.port.should == 6379
      redis.namespace.should == "driver:backbone"
    end

    describe "#register_queue" do
      it "adds queue to the set of registered queues" do
        client.register_queue("myqueue", ".*")

        redis.hgetall("registered_queues").should == {
          "myqueue" => ".*"
        }
      end
    end

    describe "#deliver" do
      it "publishes message over the message topic channel" do
        connection = Redis.new

        connection.psubscribe("*:event:helloworld") do |on|
          on.psubscribe do |pattern, total|
            client.deliver(message)
          end

          on.pmessage do |pattern, channel, received_message|
            received_message.should == message.to_json
            channel.should == "#{client.redis.namespace}:1:event:helloworld"
            connection.punsubscribe(pattern)
          end
        end
      end
      
      context "registered queue exists for message type" do
        before do
          client.register_queue("myqueue", ".*:event:helloworld")
        end

        it "adds message to the environment facet for the queue" do
          client.deliver(message)
          redis.llen("myqueue:1").should == 1
          redis.lindex("myqueue:1", 0).should == message.to_json
        end

        it "adds facet to list of active facets" do
          client.deliver(message)
          redis.smembers("myqueue:active_facets").should == ["myqueue:1"]
        end

        it "pushes facet onto facet queue" do
          client.deliver(message)
          redis.llen("myqueue:facet_queue").should == 1
          redis.lindex("myqueue:facet_queue", 0).should == "myqueue:1"
        end

        it "doesn't push onto to facet queue if currently active" do
          redis.sadd("myqueue:active_facets", "myqueue:1")
          client.deliver(message)
          redis.llen("myqueue:facet_queue").should == 0
        end
      end

      context "multiple queues exist for message type" do
        before do
          client.register_queue("myqueue", ".*:.*:helloworld")
          client.register_queue("yourqueue", ".*:event:.*world")
        end

        it "adds message for both queues" do
          client.deliver(message)
          redis.llen("myqueue:1").should == 1
          redis.llen("yourqueue:1").should == 1
        end
      end

      context "registered queue exists for another message type" do
        before do
          client.register_queue("myqueue", ".*:email:helloworld")
        end

        it "doesn't add message to the queue" do
          client.deliver(message)
          redis.llen("myqueue:1").should == 0
        end

        it "doesn't add facet to list of active facets" do
          client.deliver(message)
          redis.smembers("myqueue:active_facets").should == []
        end
      end
    end

    describe "#pull" do
      before do
        client.register_queue("myqueue", ".*:event:helloworld")
      end

      it "pulls a message off the queue using FIFO strategy" do
        client.deliver(message1 = message.merge(message: 1))
        client.deliver(message2 = message.merge(message: 2))

        client.pull("myqueue").should == message1.to_json
        client.pull("myqueue").should == message2.to_json
      end

      it "pulls from facets of the queue in a round-robin nature" do
        client.deliver(message1 = message.merge(environment_id: 1, message: 1))
        client.deliver(message2 = message.merge(environment_id: 1, message: 2))
        client.deliver(message3 = message.merge(environment_id: 2, message: 3))

        client.pull("myqueue").should == message1.to_json
        client.pull("myqueue").should == message3.to_json
        client.pull("myqueue").should == message2.to_json
      end

      it "removes facet from active list if it becomes empty" do
        client.deliver(message)

        redis.smembers("myqueue:active_facets").should == ["myqueue:1"]
        client.pull("myqueue")
        redis.smembers("myqueue:active_facets").should be_empty
      end

      it "returns nil if there are no messages to retrieve" do
        client.deliver(message)

        client.pull("myqueue").should == message.to_json
        client.pull("myqueue").should be_nil
      end
    end
  end
end
