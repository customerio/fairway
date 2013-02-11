# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'fairway/version'

Gem::Specification.new do |gem|
  gem.name          = "fairway"
  gem.version       = Fairway::VERSION
  gem.authors       = ["John Allison"]
  gem.email         = ["john@customer.io"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency("activesupport")
  gem.add_dependency("redis")
  gem.add_dependency("hiredis")
  gem.add_dependency("redis-namespace")
end
