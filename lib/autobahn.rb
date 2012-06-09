# encoding: utf-8

require 'autobahn/version'
require 'autobahn/concurrency'
require 'autobahn/cluster'
require 'autobahn/publisher'
require 'autobahn/consumer_strategy'
require 'autobahn/consumer'
require 'autobahn/transport_system'


module Autobahn
  def self.transport_system(*args)
    TransportSystem.new(*args)
  end
end

