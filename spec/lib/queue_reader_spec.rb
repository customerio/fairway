require "spec_helper"

module Driver
  describe "QueueReader" do
    describe "#initialize" do
      it "requires a Connection"
      it "requires a name"
    end

    describe "#set_priority" do
      it "requires a facet"
      it "requires a priority"
    end

    describe "#pop" do
      it "accepts a message"
      it "accepts an optional channel"
      it "accepts options"

      context "with publish: true" do
        it "publishes the message"
      end
    end
  end
end
