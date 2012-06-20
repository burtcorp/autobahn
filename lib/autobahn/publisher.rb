# encoding: utf-8

module Autobahn
  class Publisher
    def initialize(exchange_name, routing, connections, encoder, options={})
      @exchange_name = exchange_name
      @routing = routing
      @connections = connections
      @encoder = encoder
      @strategy = options[:strategy] || RandomPublisherStrategy.new
    end

    def publish(message, options={})
      rk = options[:routing_key] || select_routing_key(message)
      ex = exchanges_by_routing_key[rk]
      em = @encoder.encode(message)
      op = {:routing_key => rk, :properties => @encoder.properties}
      ex.publish(em, op)
    end

    # only for internal use
    def select_routing_key(message)
      @strategy.select_routing_key(routing_keys, message)
    end

    def introspective_routing?
      @strategy.introspective?
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

  class BatchPublisher
    def initialize(publisher, batch_options)
      @publisher = publisher
      @batch_options = batch_options
      @buffers = Hash.new { |h, k| h[k] = Concurrency::LinkedBlockingDeque.new }
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

    def publish(message)
      if @publisher.introspective_routing?
        rk = @publisher.select_routing_key(message)
      else
        rk = nil
      end
      @buffers[rk].add_last(message)
      drain
    end

    def flush!
      drain(true)
    end

    def disconnect!
      @drainer_task.cancel(false) if @drainer_task
      @scheduler.shutdown if @scheduler
      @publisher.disconnect!
    end

    private

    def batch_size
      @batch_options[:size] || 1
    end

    def drain(force=false)
      @buffers.each do |rk, buffer|
        while (buffer.size >= batch_size) || (buffer.size > 0 && force)
          # TODO: this block can be entered by two threads simultaneously since the 
          #       count will remain high until a bit further down, on the other hand
          #       the worst case result is batches with too few messages
          drain_batch(rk, buffer)
        end
      end
    end

    def drain_batch(rk, buffer)
      batch = []
      batch_size.times do
        msg = buffer.poll_first
        batch << msg if msg
      end
      @publisher.publish(batch, :routing_key => rk)
      @last_drain = Time.now
    end

    def maybe_force_drain
      return if (Time.now - @last_drain) < @batch_options[:timeout]
      drain(true)
    end
  end
end
