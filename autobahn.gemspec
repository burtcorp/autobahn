# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'autobahn/version'


Gem::Specification.new do |s|
  s.name        = 'autobahn'
  s.version     = Autobahn::VERSION.dup
  s.platform    = 'java'
  s.authors     = ['The Burt Platform Team']
  s.email       = ['platform@burtcorp.com']
  s.homepage    = 'http://github.com/burtcorp/autobahn'
  s.summary     = %q{Get the most out of RabbitMQ with the least amount of code}
  s.description = %q{Autobahn is a transport system abstraction for HotBunnies/RabbitMQ that tries to maximize message throughput rates}

  s.rubyforge_project = 'autobahn'
  
  s.add_dependency 'hot_bunnies', '1.4.0.pre4'
  s.add_dependency 'httpclient', '~> 2.2.5'
  s.add_dependency 'jruby-openssl'
  s.add_dependency 'json'

  s.files         = Dir['lib/**/*.rb']
  s.require_paths = %w(lib)
end
