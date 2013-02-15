require "spec_helper"

module Fairway
  module Sidekiq
    describe QueueFetch do
      let(:reader) { QueueReader.new(Connection.new, "fairway") }
      let(:work)   { { queue: "golf_events", type: "swing", name: "putt" }.to_json }

      it "requests work from the queue reader" do
        fetch = QueueFetch.new(reader)

        reader.stub(pull: ["fairway", work])

        unit_of_work = fetch.retrieve_work
        unit_of_work.queue_name.should == "golf_events"
        unit_of_work.message.should == work
      end

      it "allows transforming of the message into a job" do
        fetch = QueueFetch.new(reader) do |fairway_queue, message|
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
    end
  end
end
