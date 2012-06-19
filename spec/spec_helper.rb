require 'bundler/setup'
require 'autobahn'


RSpec::Matchers.define :time_out do
  def timeout
    @timeout || 10
  end

  match do |latch|
    success = latch.await(timeout, Autobahn::Concurrency::TimeUnit::SECONDS)
    success.should be_false
  end

  chain :within do |to|
    @timeout = to
  end

  failure_message_for_should do |actual|
    "expected latch to time out within #{timeout}s"
  end

  failure_message_for_should_not do |actual|
    "expected latch not to time out within #{timeout}s"
  end

  description do
    "time out within #{timeout}"
  end
end
