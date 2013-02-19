require "spec_helper"

module Fairway::Sidekiq
  describe CompositeFetch do
    describe "#initialize" do
      it "accepts a hash of fetches with priority" do
        fetch = CompositeFetch.new(fetchA: 10, fetchB: 1)
        fetch.fetches.should == [Array.new(10, :fetchA), :fetchB].flatten
      end
    end

    describe "#fetch_order" do
      let(:fetch)  { CompositeFetch.new(fetchA: 10, fetchB: 1) }

      it "should shuffle and uniq fetches" do
        fetch.fetches.should_receive(:shuffle).and_return(fetch.fetches)
        fetch.fetch_order
      end

      it "should unique fetches list" do
        fetch.fetches.length.should == 11
        fetch.fetch_order.length.should == 2
      end
    end

    describe "#retrieve_work" do
      let(:work)     { mock(:work) }
      let(:fetchA) { mock(:fetch) }
      let(:fetchB) { mock(:fetch) }
      let(:fetch)  { CompositeFetch.new(fetchA => 10, fetchB => 1) }

      before do
        fetch.stub(fetch_order: [fetchA, fetchB], sleep: nil)
      end

      it "returns work from the first fetch who has work" do
        fetchA.stub(retrieve_work: work)
        fetchB.should_not_receive(:retrieve_work)

        fetch.retrieve_work.should == work
      end

      it "attempts to retrieve work from each fetch in a non blocking fashion" do
        fetchA.should_receive(:retrieve_work).with(blocking: false)
        fetchB.should_receive(:retrieve_work).with(blocking: false)
        fetch.retrieve_work.should be_nil
      end

      it "sleeps if no work is found" do
        fetch.should_receive(:sleep).with(1)

        fetchA.stub(retrieve_work: nil)
        fetchB.stub(retrieve_work: nil)

        fetch.retrieve_work
      end
    end
  end
end
