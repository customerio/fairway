require "spec_helper"

module Fairway
  describe Facet do
    let(:connection) { Connection.new }
    let(:queue)      { Queue.new(connection, "myqueue") }
    let(:message)    { { facet: 1, topic: "event:helloworld" } }

    before do
      Fairway.config.register_queue("myqueue")
    end

    describe "#length" do
      it "returns number of messages queues for a given facet" do
        connection.deliver(message.merge(facet: 1, message: 1))
        connection.deliver(message.merge(facet: 1, message: 2))
        connection.deliver(message.merge(facet: 2, message: 3))

        Facet.new(queue, 1).length.should == 2
        Facet.new(queue, 2).length.should == 1
        Facet.new(queue, 3).length.should == 0
      end
    end

    describe "#priority" do
      let(:facet) { Facet.new(queue, 1) }

      it "defaults all facets to a priority of 1" do
        facet.priority.should == [1]
      end

      it "returns value of priority set" do
        facet.priority = 4
        facet.priority.should == [4]
      end

      context "multiple queues" do
        let(:queue) { Queue.new(connection, "myqueue2", "myqueue1") }

        it "returns priority for each queue" do
          facet.priority.should == [1, 1]

          Facet.new(Queue.new(connection, "myqueue1"), 1).priority = 2

          facet.priority.should == [1, 2]
        end
      end
    end

    describe "#priority=" do
      let(:facet) { Facet.new(queue, 1) }

      it "allows positive integer priority" do
        facet.priority = 5
        facet.priority.should == [5]
      end

      it "doesn't allow non integer priority" do
        lambda { facet.priority = "hello" }.should raise_error(Facet::InvalidPriorityError)
      end

      it "doesn't allow non integer priority" do
        lambda { facet.priority = "1.23" }.should raise_error(Facet::InvalidPriorityError)
      end

      it "doesn't allow negative priority" do
        lambda { facet.priority = -1 }.should raise_error(Facet::InvalidPriorityError)
      end

      context "multiple queues" do
        let(:queue) { Queue.new(connection, "myqueue2", "myqueue1") }

        it "sets priority on each queue" do
          facet.priority = 2
          facet.priority.should == [2, 2]
        end
      end
    end

    describe "equality" do
      it "should equal facets with same queue and names" do
        Facet.new(queue, "a").should == Facet.new(queue, "a")
      end

      it "doesn't equal facets with different queues" do
        new_queue = Queue.new(connection, "otherqueue")
        Facet.new(queue, "a").should_not == Queue.new(new_queue, "a")
      end

      it "doesn't equal queues with different queues" do
        Facet.new(queue, "a").should_not == Facet.new(queue, "b")
      end
    end
  end
end
