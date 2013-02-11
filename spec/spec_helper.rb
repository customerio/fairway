ENV["RAILS_ENV"] = "test"

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require_relative "../boot"
require "rspec/autorun"

Bundler.require(:default, :test)

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
end

require "sidekiq"
require "sidekiq/manager"
require "driver/sidekiq"

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each {|f| require f}

def clear_test_data
  redis = Driver::Config.new.redis
  redis.del(*redis.keys) if redis.keys.any?
end

RSpec.configure do |config|
  config.before(:each) do
    Driver.configure do |config|
      config.namespace = "test:backbone"
    end

    clear_test_data
  end

  config.after(:each) do
    clear_test_data
  end
end
