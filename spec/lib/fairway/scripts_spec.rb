require "spec_helper"

module Fairway
  describe Scripts do
    describe "#initialize" do
      it "requires a redis connection pool" do
        lambda {
          Scripts.new
        }.should raise_error(ArgumentError)
      end
    end

    describe "#method_missing" do
      let(:scripts) { Scripts.new(ConnectionPool.new{ Redis.new }, "foo") }

      it "runs the script" do
        scripts.fairway_register_queue("namespace", "name", "topic")
      end

      context "when the script does not exist" do
        it "loads the script" do
          Redis.new.script(:flush)
          scripts.fairway_register_queue("namespace", "name", "topic")
        end
      end
    end
  end
end
