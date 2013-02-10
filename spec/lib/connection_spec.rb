require "spec_helper"

module Driver
  describe "Connection" do
    describe "#initialize" do
      it "requires a Config"
      it "registers queues from the config"
      it "sets self.publish"

      context "when an existing queue definition does not match" do
        it "raises a QueueMismatchError"
      end
    end

    describe "#push" do
      it "requires a message"
      it "accepts a channel"
      it "accepts options"

      context "when publishing" do
        it "publishes the message"
      end
    end
  end
end
