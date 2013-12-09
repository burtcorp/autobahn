# encoding: utf-8


module Autobahn
  class TransportSystem
    attr_reader :cluster

    def initialize(api_uri, exchange_name, options={})
      @cluster = (options[:cluster_factory] || Cluster).new(api_uri, options)
      @connection_factory = options[:connection_factory] || HotBunnies
      @exchange_name = exchange_name
      @host_resolver = options[:host_resolver] || DefaultHostResolver.new
      @encoder = options[:encoder] || StringEncoder.new
      @batch_options = options[:batch]
      @logger = options[:logger] || NullLogger.new
      @consumers = []
      @publishers = []
    end

    def name
      @exchange_name
    end

    def exists?(reload=false)
      setup!(reload)
      @routing.keys.any?
    end

    def size
      setup!
      @routing.keys.size
    end

    def connected?
      !!@connections
    end

    def create!(options={})
      return if exists?(true)
      connect!
      connections = @connections.sort.map(&:last)
      queues_per_node = options[:queues_per_node] || 1
      rks_per_queue = options[:routing_keys_per_queue] || 1
      queue_prefix = options[:queue_prefix] || "#{@exchange_name}_"
      rk_prefix = options[:routing_key_prefix] || queue_prefix
      total_queue_count = connections.size * queues_per_node
      total_rk_count = total_queue_count * rks_per_queue
      queue_suffix_width = total_queue_count > 0 ? [Math.log10(total_queue_count).ceil, 2].max : 0
      rk_suffix_width = total_rk_count > 0 ? [Math.log10(total_rk_count).ceil, 2].max : 0
      exchange = connections.sample.create_channel.exchange(@exchange_name, :type => :direct, :durable => options.fetch(:durable, true))
      queue_options = {:durable => options.fetch(:durable, true), :arguments => options.fetch(:arguments, {})}
      queue_options[:arguments]['x-ha-policy'] = 'all' if options[:ha] == :all
      connections.size.times do |node_index|
        connection = connections[node_index]
        channel = connection.create_channel
        queues_per_node.times do |queue_offset|
          queue_index = node_index * queues_per_node + queue_offset
          queue_name = "#{queue_prefix}#{queue_index.to_s.rjust(queue_suffix_width, '0')}"
          queue = channel.queue(queue_name, queue_options)
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
      disconnect_channels!
      channel = @connections.values.sample.create_channel
      if cluster.exchanges.find { |e| e['name'] == @exchange_name }
        channel.exchange_delete(@exchange_name)
      end
      @routing.keys.each do |queue_name|
        channel.queue_delete(queue_name)
      end
      setup!(true)
    end

    def consumer(options={})
      setup!
      connect!
      options[:preferred_decoder] = options.delete(:encoder) if options.has_key?(:encoder) # for backwards compatibility
      options = {:logger => @logger, :preferred_decoder => @encoder}.merge(options)
      consumer = Consumer.new(@routing, @connections, Encoder, options)
      @consumers << consumer
      @consumers.last
    end

    def publisher(options={})
      setup!
      connect!
      if @batch_options
        raise ArgumentError, 'Encoder does not support batching!' unless @encoder.encodes_batches?
        options[:batch] = @batch_options.merge(options.fetch(:batch,{}))
      end
      options = {:logger => @logger}.merge(options)
      publisher = Publisher.new(@exchange_name, @routing, @connections, @encoder, options)
      publisher.start!
      @publishers << publisher
      @publishers.last
    end

    def disconnect!
      disconnect_channels!
      @connections.values.each(&:close) if @connections
      @connections = nil
    end

    private

    attr_reader :logger

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
        acc[node] = @connection_factory.connect(connection_configuration(node))
        acc
      end
    end

    def disconnect_channels!
      @publishers.each(&:disconnect!)
      @publishers = []
      @consumers.each(&:disconnect!)
      @consumers = []
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
