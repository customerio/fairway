require "spec_helper"

module Fairway::Sidekiq
  describe Fetch do
    describe "#initialize" do
      it "accepts a block to define of fetches with priority" do
        fetch = Fetch.new do |fetch|
          fetch.from :fetchA, 10
          fetch.from :fetchB, 1
        end

        fetchA = FairwayFetch.new(:fetchA)
        fetchB = FairwayFetch.new(:fetchB)

        fetch.fetches.should == [Array.new(10, fetchA), fetchB].flatten
      end

      it "instantiates a BasicFetch if you fetch from the keyword :sidekiq" do
        fetch = Fetch.new do |fetch|
          fetch.from :sidekiq, 1
        end

        fetch.fetches.length.should == 1
        fetch.fetches.first.should be_instance_of(BasicFetch)
      end

      it "instantiates a FairwayFetch if you fetch from a queue object" do
        queue = Fairway::Queue.new(Fairway::Connection.new, "fairway")

        fetch = Fetch.new do |fetch|
          fetch.from queue, 1
        end

        fetch.fetches.length.should == 1
        fetch.fetches.first.should be_instance_of(FairwayFetch)
      end
    end

    describe "#new" do
      it "returns itself to match Sidekiq fetch API" do
        fetch = Fetch.new do |fetch|
          fetch.from :fetchA, 1
        end

        fetch.new({}).should == fetch
      end
    end

    describe "#bulk_requeue" do
      it "requeues jobs to redis" do
        uow = Sidekiq::BasicFetch::UnitOfWork
        q1 = Sidekiq::Queue.new('foo')
        q2 = Sidekiq::Queue.new('bar')

        q1.size.should == 0
        q2.size.should == 0

        Fetch.bulk_requeue([uow.new('queue:foo', 'bob'), uow.new('queue:foo', 'bar'), uow.new('queue:bar', 'widget')])

        q1.size.should == 2
        q2.size.should == 1
      end

      it "requeues jobs to redis from instance" do
        uow = Sidekiq::BasicFetch::UnitOfWork
        q1 = Sidekiq::Queue.new('foo')
        q2 = Sidekiq::Queue.new('bar')

        q1.size.should == 0
        q2.size.should == 0

        Fetch.new{ |f| }.bulk_requeue([uow.new('queue:foo', 'bob'), uow.new('queue:foo', 'bar'), uow.new('queue:bar', 'widget')])

        q1.size.should == 2
        q2.size.should == 1
      end
    end

    describe "#fetch_order" do
      let(:fetch)  { Fetch.new { |f| f.from :fetchA, 10; f.from :fetchB, 1 } }

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
      let(:work)   { mock(:work)  }
      let(:fetchA) { mock(:fetch) }
      let(:fetchB) { mock(:fetch) }
      let(:fetch)  { Fetch.new { |f| f.from :fetchA, 10; f.from :fetchB, 1 } }

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

      it "doesn't sleep if blocking option is false" do
        fetch.should_not_receive(:sleep)

        fetchA.stub(retrieve_work: nil)
        fetchB.stub(retrieve_work: nil)

        fetch.retrieve_work(blocking: false)
      end
    end
  end
end
