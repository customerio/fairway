require "spec_helper"

module Fairway::Sidekiq
  describe CompositeFetch do
    describe "#initialize" do
      it "accepts a hash of fetches with priority" do
        fetcher = CompositeFetch.new(fetcherA: 10, fetcherB: 1)
        fetcher.fetches.should == [Array.new(10, :fetcherA), :fetcherB].flatten
      end
    end

    describe "#fetch_order" do
      let(:fetcher)  { CompositeFetch.new(fetcherA: 10, fetcherB: 1) }

      it "should shuffle and uniq fetches" do
        fetcher.fetches.should_receive(:shuffle).and_return(fetcher.fetches)
        fetcher.fetch_order
      end

      it "should unique fetches list" do
        fetcher.fetches.length.should == 11
        fetcher.fetch_order.length.should == 2
      end
    end

    describe "#retrieve_work" do
      let(:work)     { mock(:work) }
      let(:fetcherA) { mock(:fetcher) }
      let(:fetcherB) { mock(:fetcher) }
      let(:fetcher)  { CompositeFetch.new(fetcherA => 10, fetcherB => 1) }

      before do
        fetcher.stub(fetch_order: [fetcherA, fetcherB])
      end

      it "returns work from the first fetcher who has work" do
        fetcherA.stub(retrieve_work: work)
        fetcherB.should_not_receive(:retrieve_work)

        fetcher.retrieve_work.should == work
      end

      it "attempts to retrieve work from each fetcher if no work is found" do
        fetcherA.should_receive(:retrieve_work)
        fetcherB.should_receive(:retrieve_work)
        fetcher.retrieve_work.should be_nil
      end
    end
  end
end
