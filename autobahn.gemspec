# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'autobahn/version'


Gem::Specification.new do |s|
  s.name        = 'autobahn'
  s.version     = Autobahn::VERSION.dup
  s.license     = 'BSD-3-Clause'
  s.platform    = 'java'
  s.authors     = ['The Burt Platform Team']
  s.email       = ['platform@burtcorp.com']
  s.homepage    = 'http://github.com/burtcorp/autobahn'
  s.summary     = %q{Get the most out of RabbitMQ with the least amount of code}
  s.description = %q{Autobahn is a transport system abstraction for HotBunnies/RabbitMQ that tries to maximize message throughput rates}

  s.rubyforge_project = 'autobahn'

  s.add_dependency 'hot_bunnies', '~> 1.5'
  s.add_dependency 'httpclient', '~> 2.2'
  s.add_dependency 'json'

  s.files         = Dir['lib/**/*.rb'] + Dir['lib/**/*.jar']
  s.require_paths = %w(lib)
  s.bindir        = 'bin'
  s.executables   = %w(create-transport-system destroy-transport-system)
end
