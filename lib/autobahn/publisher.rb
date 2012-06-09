# encoding: utf-8

module Autobahn
  class Publisher
    def initialize(exchange_name, routing, connections)
      @exchange_name = exchange_name
      @routing = routing
      @connections = connections
    end

    def publish(message)
      rk = routing_keys.sample
      ex = exchanges_by_routing_key[rk]
      ex.publish(message, :routing_key => rk)
    end

    def disconnect!
      exchanges_by_routing_key.values.uniq.map(&:channel).map(&:close)
      @exchanges_by_routing_key = nil
      @nodes_by_routing_key = nil
      @routing_keys = nil
    end

    private

    def exchanges_by_routing_key
      @exchanges_by_routing_key ||= begin
        exchanges_by_node = Hash[nodes_by_routing_key.values.uniq.map { |node| [node, @connections[node].create_channel.exchange(@exchange_name, :passive => true)] }]
        Hash[nodes_by_routing_key.map { |rk, node| [rk, exchanges_by_node[node]] }]
      end
    end

    def nodes_by_routing_key
      @nodes_by_routing_key ||= Hash[@routing.flat_map { |_, r| r[:routing_keys].map { |rk| [rk, r[:node]] } }]
    end

    def routing_keys
      @routing_keys ||= nodes_by_routing_key.keys
    end
  end
end
