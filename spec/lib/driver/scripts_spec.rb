require "spec_helper"

module Driver
  describe Scripts do
    describe "#initialize" do
      it "requires a redis client" do
        lambda {
          Scripts.new
        }.should raise_error(ArgumentError)
      end
    end

    describe "#method_missing" do
      let(:scripts) { Scripts.new(Redis.new, "foo") }

      it "runs the script" do
        scripts.driver_register_queue("namespace", "name", "topic")
      end

      context "when the script does not exist" do
        it "loads the script" do
          Redis.new.script(:flush)
          scripts.driver_register_queue("namespace", "name", "topic")
        end
      end
    end
  end
end
