# encoding: utf-8

require 'hot_bunnies'


module Autobahn
  class TransportSystem
    attr_reader :cluster

    def initialize(api_uri, exchange_name, options={})
      @cluster = Cluster.new(api_uri, options)
      @exchange_name = exchange_name
      @host_resolver = options[:host_resolver] || method(:default_host_resolver)
      @consumers = []
      @publishers = []
    end

    def consumer(options={})
      setup!
      connect!
      @consumers << Consumer.new(@routing, @connections, options)
      @consumers.last
    end

    def publisher
      setup!
      connect!
      @publishers << Publisher.new(@exchange_name, @routing, @connections)
      @publishers.last
    end

    def disconnect!
      @consumers.each(&:disconnect!)
      @publishers.each(&:disconnect!)
      @connections.values.each(&:close) if @connections
    end

    private

    def setup!
      return if defined? @routing
      bindings = @cluster.bindings.select { |b| b['source'] == @exchange_name && b['destination_type'] == 'queue' }
      @routing = bindings.reduce({}) do |acc, binding|
        queue_name = binding['destination']
        acc[queue_name] ||= {:routing_keys => []}
        acc[queue_name][:routing_keys] << binding['routing_key']
        acc
      end
      @cluster.queues.each do |queue|
        queue_name = queue['name']
        if @routing.key?(queue_name)
          @routing[queue_name][:node] = queue['node']
        end
      end
    end

    def connect!
      return if defined? @connections
      nodes = @routing.values.map { |q| q[:node] }.uniq
      @connections = nodes.reduce({}) do |acc, node|
        acc[node] = HotBunnies.connect(connection_configuration(node))
        acc
      end
    end

    def default_host_resolver(node)
      node.split('@').last
    end

    def connection_configuration(node)
      host = @host_resolver.call(node)
      {:host => host, :port => node_ports[node]}
    end

    def node_ports
      @node_ports ||= begin
        pairs = @cluster.overview['listeners'].map do |l|
          if l['protocol'] == 'amqp'
            [l['node'], l['port']]
          else
            nil
          end
        end
        Hash[pairs.compact]
      end
    end
  end
end
