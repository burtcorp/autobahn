# encoding: utf-8

module Autobahn
  class Consumer
    def initialize(routing, connections, encoder_registry, options)
      @routing = routing
      @connections = connections
      @encoder_registry = encoder_registry
      @prefetch = options[:prefetch]
      @buffer_size = options[:buffer_size]
      @strategy = options[:strategy] || DefaultConsumerStrategy.new
      check_buffer_size!
      @demultiplexer = options[:demultiplexer]
      unless @demultiplexer
        if @buffer_size
          @demultiplexer = BlockingQueueDemultiplexer.new(@buffer_size)
        else
          @demultiplexer = BlockingQueueDemultiplexer.new
        end
      end
      @demultiplexer = @demultiplexer
      @setup = Concurrency::AtomicBoolean.new(false)
      @deliver = Concurrency::AtomicBoolean.new(false)
      @subscribed = Concurrency::AtomicBoolean.new(false)
      @setup_lock = Concurrency::Lock.new
    end

    def subscribe(options={}, &handler)
      raise 'Subscriber already registered' unless @subscribed.compare_and_set(false, true)
      setup!
      mode = options[:mode] || :async
      timeout = options[:timeout] || 1
      case mode
      when :async
        @deliver_pool = create_thread_pool(1)
        @deliver_pool.execute { deliver(handler, timeout) }
      when :blocking
        deliver(handler, timeout)
      else
        raise ArgumentError, "Not a valid subscription mode: #{mode}"
      end
      nil
    end

    def next(timeout=nil)
      setup!
      @demultiplexer.take(timeout)
    end

    def ack(consumer_tag, delivery_tag, options={})
      find_subscription(consumer_tag).basic_ack(delivery_tag, !!options[:multiple])
    end

    def reject(consumer_tag, delivery_tag, options={})
      find_subscription(consumer_tag).basic_reject(delivery_tag, !!options[:requeue])
    end

    def unsubscribe!(timeout=30)
      @setup_lock.lock do
        @subscriptions.each(&:cancel) if @subscriptions
        @subscriptions = nil
      end
    end

    def disconnect!(timeout=30)
      @setup_lock.lock do
        unsubscribe!(timeout)
        if @deliver.get
          @queues.map(&:channel).each(&:close) if @queues
          @queues = nil
          @deliver.set(false)
          @deliver_pool_timeout = false
          if @deliver_pool
            unless Concurrency.shutdown_thread_pool!(@deliver_pool, 1, timeout)
              raise 'Could not shut down delivery thread pool'
            end
            @deliver_pool = nil
            @demultiplexer = nil
          end
        end
      end
    end

    private

    def check_buffer_size!
      if @buffer_size
        subscription_count = @routing.size.times.select { |i| @strategy.subscribe?(i, @routing.size) }.size
        if @buffer_size < subscription_count
          raise ArgumentError, sprintf('Buffer size too small: %d (will subscribe to %d queues)', @buffer_size, subscription_count)
        end
      end
    end

    def setup!
      if @setup.compare_and_set(false, true)
        @setup_lock.lock do
          @queues = create_queues
          @subscriptions = create_subscriptions
          @subscriptions.each do |subscription|
            queue_consumer = QueueingConsumer.new(subscription.channel, @encoder_registry, @demultiplexer)
            subscription.start(queue_consumer)
          end
          @deliver.set(true)
        end
      end
    end

    def deliver(handler, timeout)
      begin
        headers, message = self.next(timeout)
        handler.call(headers, message) if headers
      end while @deliver.get
    end

    def channels_by_consumer_tag
      @channels_by_consumer_tag ||= Hash[@subscriptions.map { |s| [s.consumer_tag, s.channel] }]
    end

    def create_subscriptions
      @queues.map { |queue| queue.subscribe(:ack => true) }
    end

    def create_queues
      @routing.sort.each_with_index.map do |(queue_name, meta), i|
        if @strategy.subscribe?(i, @routing.size)
          channel = @connections[meta[:node]].create_channel
          channel.prefetch = @prefetch if @prefetch
          channel.queue(queue_name, :passive => true)
        else
          nil
        end
      end.compact
    end

    def find_subscription(consumer_tag)
      subscription = channels_by_consumer_tag[consumer_tag]
      raise ArgumentError, %(Invalid consumer tag: #{consumer_tag}) unless subscription
      subscription
    end

    def create_thread_pool(size)
      thread_factory = Concurrency::NamingDaemonThreadFactory.new('autobahn')
      Concurrency::Executors.new_fixed_thread_pool(size, thread_factory)
    end
  end

  class BatchHeaders
    def initialize(*args)
      @headers, @batch_size = args
      @acks = Concurrency::AtomicInteger.new
    end

    def ack(options={})
      if @batch_size == @acks.increment_and_get
        @headers.ack(options)
      end
    end

    def reject(options={})
      raise 'Not yet implemented'
      @headers.reject(options)
    end

    def responds_to?(name)
      super unless @headers.responds_to?(name)
    end

    def method_missing(name, *args, &block)
      super unless @headers.responds_to?(name)
      @headers.method_missing?(name, *args, &block)
    end
  end

  class QueueingConsumer < HotBunnies::Queue::BaseConsumer
    def initialize(channel, encoder_registry, demultiplexer)
      super(channel)
      @encoder_registry = encoder_registry
      @demultiplexer = demultiplexer
    end

    def deliver(*pair)
      headers, encoded_message = pair
      encoder = @encoder_registry[headers.content_type, :content_encoding => headers.content_encoding]
      decoded_message = encoder.decode(encoded_message)
      if encoder.encodes_batches? && decoded_message.is_a?(Array)
        if decoded_message.size == 1
          pair[1] = decoded_message.first
          @demultiplexer.put(pair)
        else
          batch_headers = BatchHeaders.new(headers, decoded_message.size)
          decoded_message.each do |message|
            @demultiplexer.put([batch_headers, message])
          end
        end
      else
        pair[1] = decoded_message
        @demultiplexer.put(pair)
      end
    end
  end

  class BlockingQueueDemultiplexer
    def initialize(buffer_size=nil)
      if buffer_size
        @queue = Concurrency::ArrayBlockingQueue.new(buffer_size)
      else
        @queue = Concurrency::LinkedBlockingQueue.new
      end
    end

    def put(pair)
      @queue.put(pair)
    end

    def take(timeout=nil)
      if timeout
        @queue.poll((timeout * 1000).to_i, Concurrency::TimeUnit::MILLISECONDS)
      else
        @queue.take
      end
    end
  end
end
