# encoding: utf-8

require 'hot_bunnies'


module Autobahn
  class TransportSystem
    attr_reader :cluster

    def initialize(api_uri, exchange_name, options={})
      @cluster = (options[:cluster_factory] || Cluster).new(api_uri, options)
      @exchange_name = exchange_name
      @host_resolver = options[:host_resolver] || DefaultHostResolver.new
      @encoder = options[:encoder] || StringEncoder.new
      @batch_options = options[:batch]
      @consumers = []
      @publishers = []
    end

    def exists?
      setup!
      @routing.keys.any?
    end

    def size
      setup!
      @routing.keys.size
    end

    def create!(options={})
      return if exists?
      connect!
      connections = @connections.sort.map(&:last)
      queues_per_node = options[:queues_per_node] || 1
      rks_per_queue = options[:routing_keys_per_queue] || 1
      queue_prefix = options[:queue_prefix] || "#{@exchange_name}_"
      rk_prefix = options[:routing_key_prefix] || queue_prefix
      queue_suffix_width = [Math.log10(connections.size * queues_per_node).ceil, 2].max
      rk_suffix_width = [Math.log10(connections.size * queues_per_node * rks_per_queue).ceil, 2].max
      exchange = connections.sample.create_channel.exchange(@exchange_name, :type => :direct)
      connections.size.times do |node_index|
        connection = connections[node_index]
        channel = connection.create_channel
        queues_per_node.times do |queue_offset|
          queue_index = node_index * queues_per_node + queue_offset
          queue_name = "#{queue_prefix}#{queue_index.to_s.rjust(queue_suffix_width, '0')}"
          queue = channel.queue(queue_name)
          rks_per_queue.times do |rk_offset|
            rk_index = queue_index * rks_per_queue + rk_offset
            rk_name = "#{rk_prefix}#{rk_index.to_s.rjust(rk_suffix_width, '0')}"
            queue.bind(exchange, :routing_key => rk_name)
          end
        end
      end
      setup!(true)
    end

    def destroy!
      setup!(true)
      connect!
      channel = @connections.values.sample.create_channel
      if cluster.exchanges.find { |e| e['name'] == @exchange_name }
        channel.exchange_delete(@exchange_name)
      end
      @routing.keys.each do |queue_name|
        channel.queue_delete(queue_name)
      end
    end

    def consumer(options={})
      setup!
      connect!
      consumer = Consumer.new(@routing, @connections, @encoder, options)
      consumer = BatchConsumer.new(consumer, @batch_options) if @batch_options
      @consumers << consumer
      @consumers.last
    end

    def publisher(options={})
      setup!
      connect!
      raise ArgumentError, 'Encoder does not support batching!' if @batch_options && !@encoder.encodes_batches?
      publisher = Publisher.new(@exchange_name, @routing, @connections, @encoder, options)
      publisher = BatchPublisher.new(publisher, @batch_options).start! if @batch_options
      @publishers << publisher
      @publishers.last
    end

    def disconnect!
      @consumers.each(&:disconnect!)
      @publishers.each(&:disconnect!)
      @connections.values.each(&:close) if @connections
    end

    private

    class DefaultHostResolver
      def resolve(node)
        node.split('@').last
      end
    end

    def setup!(reload=false)
      @routing = nil if reload
      return if @routing
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
      if nodes.empty?
        nodes = @cluster.nodes.map { |n| n['name'] }
      end
      @connections = nodes.reduce({}) do |acc, node|
        acc[node] = HotBunnies.connect(connection_configuration(node))
        acc
      end
    end

    def connection_configuration(node)
      host = @host_resolver.resolve(node)
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
