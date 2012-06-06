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

    def initialize(exchange_name, options={})
      @cluster = Cluster.new(options)
      @exchange_name = exchange_name
      @connection_configurator = options[:connection_configurator] || method(:default_connection_configurator)
      @consumers = []
      @publishers = []
    end

    def consumer
      setup!
      connect!
      @consumers << Consumer.new(@routing, @connections)
      @consumers.last
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
      @connections = nodes.reduce({}) do |acc, host|
        acc[host] = HotBunnies.connect(@connection_configurator.call(host))
        acc
      end
    end

    def default_connection_configurator(host)
      {:host => host}
    end
  end

  class Consumer
    def initialize(routing, connections)
      @routing = routing
      @connections = connections
    end

    def subscribe(options={}, &handler)
      return if defined? @subscriptions
      internal_queue = Concurrency::LinkedBlockingQueue.new
      @subscriber_pool = create_thread_pool(queues.size + (options[:blocking] ? 0 : 1))
      @subscriptions = queues.map { |queue| queue.subscribe(:ack => true) }
      @subscriptions.each do |subscription|
        subscription.each(:blocking => false, :executor => @subscriber_pool) do |headers, message|
          internal_queue.put([headers, message])
        end
      end
      if options[:blocking]
        deliver(internal_queue, handler)
      else
        @subscriber_pool.execute { deliver(internal_queue, handler) }
      end
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
      if @subscriber_pool
        @subscriber_pool.shutdown
        if @subscriber_pool.await_termination(5, Concurrency::TimeUnit::SECONDS)
          @subscriber_pool = nil
        else
          # TODO: log an error
        end
      end
    end

  private

    def deliver(internal_queue, handler)
      @running = true
      while @running
        headers, message = internal_queue.poll(1, Concurrency::TimeUnit::SECONDS)
        handler.call(headers, message) if headers
      end
    end

    def queues
      @queues ||= @routing.map do|queue_name, meta|
        channel = @connections[meta[:node]].create_channel
        channel.prefetch = 10
        channel.queue(queue_name, :passive => true)
      end
    end

    def create_thread_pool(size)
      thread_factory = NamingDaemonThreadFactory.new('autobahn')
      Concurrency::Executors.new_fixed_thread_pool(size, thread_factory)
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
      def entity(name)
        plural = "#{name}s".to_sym
        define_method(plural) { api_call(plural) }
        define_method(name) { |id| api_call(plural, id) }
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

    def initialize(options={})
      api_host = options[:api_host] || 'localhost'
      api_port = options[:api_port] || 55672
      user = options[:user] || 'guest'
      password = options[:password] || 'guest'
      configure_rest_client("http://#{api_host}:#{api_port}/api", user, password)
    end
  end
end