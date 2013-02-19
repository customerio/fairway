require "spec_helper"

module Fairway::Sidekiq
  describe BasicFetch do
    let(:queues) { [:critical, :critical, :default] }
    let(:fetch)  { BasicFetch.new(queues: queues) }

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

      it "sleeps if no work is found" do
        fetch.should_receive(:sleep).with(1)
        fetch.retrieve_work
      end

      it "doesn't sleep if blocking option is false" do
        fetch.should_not_receive(:sleep)
        fetch.retrieve_work(blocking: false)
      end
    end
  end
end
