require "spec_helper"

module Fairway::Sidekiq
  describe Fetcher do
    let(:manager) { mock(:manager) }
    let(:fetch) { mock(:fetch) }

    it "accepts a manager and a fetch strategy" do
      fetcher = Fetcher.new(manager, fetch)
      fetcher.mgr.should == manager
      fetcher.strategy.should == fetch
    end

    describe "#fetch" do
      let(:fetcher) { Fetcher.new(manager, fetch) }

      it "retrieves work from fetch strategy" do
        fetch.should_receive(:retrieve_work)
        fetcher.fetch
      end

      it "tells manager to assign work if work is fetched" do
        work = mock(:work)
        fetch.stub(retrieve_work: work)
        manager.stub(async: manager)
        manager.should_receive(:assign).with(work)
        fetcher.fetch
      end
    end
  end
end
