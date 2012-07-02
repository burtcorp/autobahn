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
      @batch_buffers = Hash.new { |h, k| h[k] = Concurrency::LinkedBlockingDeque.new }
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
      @batch_buffers[rk].add_last(message)
      drain
    end

    def flush!
      drain(true)
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

    def drain(force=false)
      @batch_buffers.each do |rk, buffer|
        while (buffer.size >= @batch_options[:size]) || (buffer.size > 0 && force)
          # TODO: this block can be entered by two threads simultaneously since the 
          #       count will remain high until a bit further down, on the other hand
          #       the worst case result is batches with too few messages
          drain_batch(rk, buffer)
        end
      end
    end

    def drain_batch(rk, buffer)
      if @encoder.encodes_batches?
        batch = []
        @batch_options[:size].times do
          msg = buffer.poll_first
          batch << msg if msg
        end
        publish_raw(batch, rk)
      else
        @batch_options[:size].times do
          msg = buffer.poll_first
          publish_raw(msg, rk) if msg
        end
      end
      @last_drain = Time.now
    end

    def maybe_force_drain
      return if (Time.now - @last_drain) < @batch_options[:timeout]
      drain(true)
    end

    def publish_raw(message, rk)
      rk ||= @strategy.select_routing_key(routing_keys, message)
      ex = exchanges_by_routing_key[rk]
      em = @encoder.encode(message)
      op = {:routing_key => rk, :properties => @encoder.properties}
      ex.publish(em, op)
    end
  end
end
