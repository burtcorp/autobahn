require 'bundler/setup'
require 'autobahn'

RSpec::Matchers.define :time_out do
  match do |latch|
    timeout = @timeout || 10
    latch.await(timeout, Autobahn::Concurrency::TimeUnit::SECONDS).should be_false
  end

  chain :within do |timeout|
    @timeout = timeout
  end
end
