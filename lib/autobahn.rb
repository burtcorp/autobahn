# encoding: utf-8

require 'httpclient'
require 'hot_bunnies'
require 'json'


module Autobahn
  module Concurrency
    import 'java.lang.Thread'
    import 'java.util.concurrent.atomic.AtomicInteger'
    import 'java.util.concurrent.ThreadFactory'
    import 'java.util.concurrent.Executors'
    import 'java.util.concurrent.LinkedBlockingQueue'
    import 'java.util.concurrent.TimeUnit'
    import 'java.util.concurrent.CountDownLatch'
  end

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

  class SubsetConsumerStrategy
    def initialize(consumer_index, num_consumers, mode=:range)
      @consumer_index, @num_consumers, @mode = consumer_index, num_consumers, mode
    end

    def subscribe?(index, total_count)
      if @mode == :modulo
        index % @num_consumers == @consumer_index
      else
        range_width = total_count/@num_consumers
        start_index = range_width * @consumer_index
        index >= start_index && index < start_index + range_width
      end
    end
  end

  class DefaultConsumerStrategy
    def subscribe?(index, total_count)
      true
    end
  end

  class Consumer
    def initialize(routing, connections, options)
      @routing = routing
      @connections = connections
      @options = options
      @strategy = options[:strategy] || DefaultConsumerStrategy.new
      @extra_workers = 0
      @subscribed = false
      @running = false
    end

    def setup!
      return if @subscribed
      subscriptions.each do |subscription|
        subscription.each(:blocking => false, :executor => worker_pool) do |headers, message|
          internal_queue.put([headers, message])
        end
      end
      @running = true
      @subscribed = true
    end

    def subscribe(options={}, &handler)
      raise 'Already subscribed!' if @subscribed
      mode = options[:mode] || :async
      case mode
      when :async
        @extra_workers += 1
        worker_pool.execute { deliver(handler) }
      when :blocking
        deliver(handler)
      else
        raise ArgumentError, "Not a valid subscription mode: #{mode}"
      end
      nil
    end

    def next(timeout=nil)
      setup!
      if timeout
        internal_queue.poll((timeout * 1000).to_i, Concurrency::TimeUnit::MILLISECONDS)
      else
        internal_queue.take
      end
    end

    def ack(consumer_tag, delivery_tag, options={})
      find_subscription(consumer_tag).basic_ack(delivery_tag, !!options[:multiple])
    end

    def reject(consumer_tag, delivery_tag, options={})
      find_subscription(consumer_tag).basic_reject(delivery_tag, !!options[:requeue])
    end

    def disconnect!
      return unless @running
      @running = false
      if @subscriptions
        @subscriptions.each(&:cancel)
        @subscriptions = nil
      end
      if @queues
        @queues.map(&:channel).each(&:close)
        @queues = nil
      end
      if @worker_pool
        @worker_pool.shutdown
        unless @worker_pool.await_termination(5, Concurrency::TimeUnit::SECONDS)
          # TODO: log an error
        end
        @worker_pool = nil
      end
    end

    private

    def deliver(handler)
      begin
        headers, message = self.next(1)
        handler.call(headers, message) if headers
      end while @running
    end

    def worker_pool
      @worker_pool ||= create_thread_pool(queues.size + @extra_workers)
    end

    def subscriptions
      @subscriptions ||= queues.map { |queue| queue.subscribe(:ack => true) }
    end

    def channels_by_consumer_tag
      @channels_by_consumer_tag ||= Hash[subscriptions.map { |s| [s.consumer_tag, s.channel] }]
    end

    def internal_queue
      @internal_queue ||= Concurrency::LinkedBlockingQueue.new
    end

    def queues
      @queues ||= @routing.sort.each_with_index.map do |(queue_name, meta), i|
        if @strategy.subscribe?(i, @routing.size)
          channel = @connections[meta[:node]].create_channel
          channel.prefetch = @options[:prefetch] if @options[:prefetch]
          channel.queue(queue_name, :passive => true)
        else
          nil
        end
      end.compact
    end

    def find_subscription(consumer_tag)
      subscription = channels_by_consumer_tag[consumer_tag]
      raise ArgumentError, 'Invalid consumer tag' unless subscription
      subscription
    end

    def create_thread_pool(size)
      thread_factory = NamingDaemonThreadFactory.new('autobahn')
      Concurrency::Executors.new_fixed_thread_pool(size, thread_factory)
    end
  end

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

  class NamingDaemonThreadFactory
    include Concurrency::ThreadFactory

    def self.next_id
      @id ||= Concurrency::AtomicInteger.new
      @id.get_and_increment
    end

    def initialize(base_name)
      @base_name = base_name
    end

    def newThread(runnable)
      t = Concurrency::Thread.new(runnable)
      t.daemon = true
      t.name = "#@base_name-#{self.class.next_id}"
      t
    end
  end

  module RestClient
    module InstanceMethods
      def configure_rest_client(base_url, user, password)
        @base_url = base_url
        @http_client = HTTPClient.new
        @http_client.set_auth(@base_url, user, password)
      end

      def api_call(entity, id=nil)
        url = "#@base_url/#{entity}"
        url << "/#{id}" if id
        JSON.parse(@http_client.get_content(url))
      end
    end

    module ClassMethods
      def entity(name, options={})
        if options[:singleton]
          define_method(name) { api_call(name) }
        else
          plural = "#{name}s".to_sym
          define_method(plural) { api_call(plural) }
          define_method(name) { |id| api_call(plural, id) }
        end
      end
    end

    def self.included(m)
      m.send(:include, InstanceMethods)
      m.send(:extend, ClassMethods)
    end
  end

  class Cluster
    include RestClient

    entity :nodes
    entity :channel
    entity :exchange
    entity :queue
    entity :binding
    entity :overview, :singleton => true

    def initialize(api_uri, options={})
      user = options[:user] || 'guest'
      password = options[:password] || 'guest'
      configure_rest_client(api_uri, user, password)
    end
  end
end