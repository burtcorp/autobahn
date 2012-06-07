# encoding: utf-8

Gem::Specification.new do |s|
  s.name        = 'autobahn'
  s.version     = '1.0.0'
  s.platform    = 'java'
  s.authors     = ['The Burt Platform Team']
  s.email       = ['platform@burtcorp.com']
  s.homepage    = 'http://github.com/burtcorp/autobahn'
  s.summary     = %q{}
  s.description = %q{}

  s.rubyforge_project = 'autobahn'
  
  s.add_dependency 'hot_bunnies', '~> 1.3.8'
  s.add_dependency 'httpclient', '~> 2.2.5'
  s.add_dependency 'jruby-openssl'
  s.add_dependency 'json'

  s.files         = Dir['lib/**/*.rb']
  s.require_paths = %w(lib)
end
