# encoding: utf-8

require 'java'


module Autobahn
  def self.transport_system(*args)
    TransportSystem.new(*args)
  end

  class NullLogger
    [:debug, :info, :warn, :error, :fatal].each do |level|
      define_method(level) { |message = nil| }
    end
  end
end

require 'hot_bunnies'
require 'autobahn/version'
require 'autobahn/concurrency'
require 'autobahn/cluster'
require 'autobahn/encoder'
require 'autobahn/publisher_strategy'
require 'autobahn/publisher'
require 'autobahn/consumer_strategy'
require 'autobahn/consumer'
require 'autobahn/transport_system'