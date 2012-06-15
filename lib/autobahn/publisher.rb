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

    def publish(message)
      rk = @strategy.select_routing_key(routing_keys, message)
      ex = exchanges_by_routing_key[rk]
      em = @encoder.encode(message)
      ex.publish(em, :routing_key => rk)
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
      @buffer = Concurrency::LinkedBlockingDeque.new
      @buffer_size = Concurrency::AtomicInteger.new
      @scheduler = Concurrency::Executors.new_single_thread_scheduled_executor(Concurrency::NamingDaemonThreadFactory.new('batch_drainer'))
    end

    def start!
      @drainer_task = @scheduler.schedule_with_fixed_delay(
        method(:force_drain).to_proc, 
        @batch_options.timeout, 
        @batch_options.timeout, 
        Concurrency::TimeUnit::SECONDS
      )
      self
    end

    def publish(message)
      @buffer.add_last(message)
      @buffer_size.increment_and_get
      drain
    end

    def disconnect!
      @drainer_task.cancel(false) if @drainer_task
      @scheduler.shutdown
      @publisher.disconnect!
    end

    private

    def drain
      while @buffer_size.get >= @batch_options.size
        # TODO: this block can be entered by two threads simultaneously since the 
        #       count will remain high until a bit further down, on the other hand
        #       the worst case result is batches with too few messages
        drain_batch
      end
    end

    def drain_batch
      batch = []
      @batch_options.size.times do
        msg = @buffer.poll_first
        if msg
          batch << msg 
          @buffer_size.decrement_and_get
        end
      end
      @publisher.publish(batch)
    end

    def force_drain
      drain
      drain_batch
    end
  end
end
