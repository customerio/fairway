require "spec_helper"

module Fairway::Sidekiq
  describe NonBlockingFetch do
    let(:queues) { [:critical, :critical, :default] }
    let(:fetch)  { NonBlockingFetch.new(queues: queues) }

    it "accepts options with a list of queues and their weights" do
      fetch.queues.should == ["queue:critical", "queue:critical", "queue:default"]
    end

    describe  "#retrieve_work" do
      it "calls rpop script with queue order" do
        fetch.stub(queues_cmd: ["queue:default", "queue:critical"])

        ::Sidekiq.redis do |conn|
          conn.lpush("queue:default", "default")
          conn.lpush("queue:critical", "critical")
        end

        unit_of_work = fetch.retrieve_work
        unit_of_work.queue_name.should == "default"
        unit_of_work.message.should == "default"

        unit_of_work = fetch.retrieve_work
        unit_of_work.queue_name.should == "critical"
        unit_of_work.message.should == "critical"
      end
    end
  end
end
