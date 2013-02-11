require "spec_helper"

module Fairway
  describe QueueReader do
    let(:config) do
      Config.new do |c|
        c.facet { |message| message[:facet] }
      end
    end
    let(:connection) do
      c = Connection.new(config)
      ChanneledConnection.new(c) do |message|
        message[:topic]
      end
    end
    let(:message)     { { facet: 1, topic: "event:helloworld" } }

    describe "#initialize" do
      it "requires a Connection and queue names" do
        lambda { QueueReader.new }.should raise_error(ArgumentError)
      end
    end

    describe "#pull" do
      before do
        config.register_queue("myqueue", "event:helloworld")
      end

      it "pulls a message off the queue using FIFO strategy" do
        connection.deliver(message1 = message.merge(message: 1))
        connection.deliver(message2 = message.merge(message: 2))

        reader = QueueReader.new(connection, "myqueue")
        reader.pull.should == message1.to_json
        reader.pull.should == message2.to_json
      end

      it "pulls from facets of the queue in a round-robin nature" do
        connection.deliver(message1 = message.merge(facet: 1, message: 1))
        connection.deliver(message2 = message.merge(facet: 1, message: 2))
        connection.deliver(message3 = message.merge(facet: 2, message: 3))

        reader = QueueReader.new(connection, "myqueue")
        reader.pull.should == message1.to_json
        reader.pull.should == message3.to_json
        reader.pull.should == message2.to_json
      end

      it "removes facet from active list if it becomes empty" do
        connection.deliver(message)

        config.redis.smembers("myqueue:active_facets").should == ["1"]
        reader = QueueReader.new(connection, "myqueue")
        reader.pull
        config.redis.smembers("myqueue:active_facets").should be_empty
      end

      it "returns nil if there are no messages to retrieve" do
        connection.deliver(message)

        reader = QueueReader.new(connection, "myqueue")
        reader.pull.should == message.to_json
        reader.pull.should be_nil
      end

      context "pulling from multiple queues" do
        before do
          config.register_queue("myqueue1", "event:1")
          config.register_queue("myqueue2", "event:2")
        end

        it "pulls messages off first queue with a message" do
          connection.deliver(message1 = message.merge(topic: "event:1"))
          connection.deliver(message2 = message.merge(topic: "event:2"))

          reader = QueueReader.new(connection, "myqueue2", "myqueue1")
          reader.pull.should == message2.to_json
          reader.pull.should == message1.to_json
        end

        it "returns nil if no queues have messages" do
          reader = QueueReader.new(connection, "myqueue2", "myqueue1")
          reader.pull.should be_nil
        end

        it "pulls from facets of the queue in a round-robin nature" do
          connection.deliver(message1 = message.merge(facet: 1, topic: "event:1"))
          connection.deliver(message2 = message.merge(facet: 1, topic: "event:1"))
          connection.deliver(message3 = message.merge(facet: 2, topic: "event:1"))
          connection.deliver(message4 = message.merge(facet: 1, topic: "event:2"))

          reader = QueueReader.new(connection, "myqueue2", "myqueue1")
          reader.pull.should == message4.to_json
          reader.pull.should == message1.to_json
          reader.pull.should == message3.to_json
          reader.pull.should == message2.to_json
        end
      end
    end
  end
end
