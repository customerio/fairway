require "spec_helper"

module Fairway
  module Sidekiq
    describe Fetch do
      let(:reader) { QueueReader.new(Connection.new, "fairway") }
      let(:work)   { { queue: "golf_events", type: "swing", name: "putt" }.to_json }
      let(:fetch)  { Fetch.new(reader) }

      it "requests work from the queue reader" do
        reader.stub(pull: ["fairway", work])

        unit_of_work = fetch.retrieve_work
        unit_of_work.queue_name.should == "golf_events"
        unit_of_work.message.should == work
      end

      it "allows transforming of the message into a job" do
        fetch = Fetch.new(reader) do |fairway_queue, message|
          {
            "queue" => "my_#{message["queue"]}",
            "class" => "GolfEventJob"
          }
        end

        reader.stub(pull: ["fairway", work])

        unit_of_work = fetch.retrieve_work
        unit_of_work.queue_name.should == "my_golf_events"
        unit_of_work.message.should == { "queue" => "my_golf_events", "class" => "GolfEventJob" }.to_json
      end

      it "sleeps if no work is found" do
        fetch.should_receive(:sleep).with(1)
        reader.stub(pull: nil)
        fetch.retrieve_work
      end

      it "doesn't sleep if blocking option is false" do
        fetch.should_not_receive(:sleep)
        reader.stub(pull: nil)
        fetch.retrieve_work(blocking: false)
      end
    end
  end
end
