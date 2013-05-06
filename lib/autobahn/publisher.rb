# encoding: utf-8

module Autobahn
  class Publisher
    def initialize(exchange_name, routing, connections, encoder, options={})
      @exchange_name = exchange_name
      @routing = routing
      @connections = connections
      @encoder = encoder
      @strategy = options[:strategy] || RandomPublisherStrategy.new
      @batch_options = options[:batch] || {:size => 1}
      @logger = options[:logger] || NullLogger.new
      @publish_properties = options[:publish] || {}
      @publish_properties[:persistent] = true if options[:persistent]
      @buffer_locks = {nil => Concurrency::ReentrantLock.new}
      @batch_buffers = {nil => []}
      routing_keys.each do |rk|
        @buffer_locks[rk] = Concurrency::ReentrantLock.new
        @batch_buffers[rk] = []
      end
    end

    def start!
      if @batch_options[:timeout] && @batch_options[:timeout] > 0
        @scheduler = Concurrency::Executors.new_single_thread_scheduled_executor(Concurrency::NamingDaemonThreadFactory.new('batch_drainer'))
        @drainer_task = @scheduler.schedule_with_fixed_delay(
          method(:maybe_force_drain).to_proc, 
          @batch_options[:timeout], 
          @batch_options[:timeout], 
          Concurrency::TimeUnit::SECONDS
        )
        @last_drain = Time.now
      end
      self
    end

    def publish(message, options={})
      rk = nil
      if @batch_options && @strategy.introspective?
        rk = @strategy.select_routing_key(routing_keys, message)
      end
      publish_internal(message, rk)
    end

    def broadcast(message)
      routing_keys.each do |rk|
        publish_internal(message, rk)
      end
    end

    def flush!
      @batch_buffers.each_key do |rk|
        with_buffer_exclusively(rk) do |buffer|
          drain_buffer(buffer, rk)
        end
      end
      @last_drain = Time.now
    end

    def disconnect!
      @drainer_task.cancel(false) if @drainer_task
      @scheduler.shutdown if @scheduler
      exchanges_by_routing_key.values.uniq.map(&:channel).map(&:close)
      @exchanges_by_routing_key = nil
      @nodes_by_routing_key = nil
      @routing_keys = nil
    end

    private

    attr_reader :logger

    def with_buffer_exclusively(rk)
      @buffer_locks[rk].lock
      yield @batch_buffers[rk]
    ensure
      @buffer_locks[rk].unlock
    end

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

    def maybe_force_drain
      return if (Time.now - @last_drain) < @batch_options[:timeout]
      flush!
    end

    def drain_buffer(buffer, rk)
      batch = buffer.shift(buffer.size)
      if @encoder.encodes_batches?
        publish_raw(batch, rk) unless batch.empty?
      else
        batch.each { |m| publish_raw(m, rk) }
      end
    end

    def publish_internal(message, rk)
      with_buffer_exclusively(rk) do |buffer|
        buffer.push(message)
        if buffer.size >= @batch_options[:size]
          drain_buffer(buffer, rk)
        end
      end
    end

    def publish_raw(message, rk)
      rk ||= @strategy.select_routing_key(routing_keys, message)
      ex = exchanges_by_routing_key[rk]
      em = @encoder.encode(message)
      op = {:routing_key => rk, :properties => @publish_properties.merge(@encoder.properties)}
      ex.publish(em, op)
    end
  end
end
