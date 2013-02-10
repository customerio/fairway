require "spec_helper"

module Driver
  describe "ChanneledConnection" do
    describe "#initialize" do
      it "requires a Connection"
      it "reqires a block for transforming messages to channel"
    end

    describe "#push" do
      it "computes the channel"
      it "delegates to Connection"
    end
  end
end
