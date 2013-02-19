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
require "fairway/sidekiq"

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each {|f| require f}

RSpec.configure do |config|
  config.before(:each) do
    Fairway.reconfigure do |config|
      config.namespace = "test:fairway"
      config.facet { |message| message[:facet] }
    end

    Fairway::Config.new.redis.flushdb
  end

  config.after(:each) do
    Fairway::Config.new.redis.flushdb
  end
end
