require "rubygems"

# Set up gems listed in the Gemfile.
ENV["BUNDLE_GEMFILE"] ||= File.join(File.dirname(__FILE__), "Gemfile")

require "bundler/setup"
Bundler.require(:default)
