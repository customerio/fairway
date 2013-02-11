require "spec_helper"

module Driver
  describe Connection do
    let(:config) do
      Config.new do |c|
        c.facet { |message| message[:facet] }
      end
    end

    let(:connection) { Connection.new(config) }
    let(:redis)      { config.redis }
    let(:message)    { { facet: 1, topic: "event:helloworld" } }

    describe "#initialize" do
      it "registers queues from the config" do
        config = Config.new
        config.register_queue("myqueue", ".*")
        config.redis.hgetall("registered_queues").should == {}
        Connection.new(config)

        config.redis.hgetall("registered_queues").should == {
          "myqueue" => ".*"
        }
      end

      context "when an existing queue definition does not match" do
        it "raises a QueueMismatchError"
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
            channel.should == "default"
            redis.punsubscribe(pattern)
          end
        end
      end

      context "registered queue exists for message type" do
        before do
          config.register_queue("myqueue")
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
    end
  end
end
