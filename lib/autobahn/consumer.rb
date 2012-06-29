# encoding: utf-8

module Autobahn
  class Consumer
    def initialize(routing, connections, encoder, options)
      @routing = routing
      @connections = connections
      @encoder = encoder
      @prefetch = options[:prefetch]
      @buffer_size = options[:buffer_size]
      if @buffer_size && @buffer_size < @routing.size
        raise ArgumentError, sprintf('Buffer size too small: %d (there are %d queues)', @buffer_size, @routing.size)
      end
      @strategy = options[:strategy] || DefaultConsumerStrategy.new
      @setup = Concurrency::AtomicBoolean.new(false)
      @deliver = Concurrency::AtomicBoolean.new(false)
      @subscribed = Concurrency::AtomicBoolean.new(false)
      @setup_lock = Concurrency::Lock.new
    end

    def subscribe(options={}, &handler)
      raise 'Subscriber already registered' unless @subscribed.compare_and_set(false, true)
      setup!
      mode = options[:mode] || :async
      # TODO: this is a bit ugly, and is only here to support batching, 
      #       this class shouldn't have to worry about that!
      consumer = options[:consumer] || self 
      timeout = options[:timeout] || 1
      case mode
      when :async
        @deliver_pool = create_thread_pool(1)
        @deliver_pool.execute { deliver(consumer, handler, timeout) }
      when :blocking
        deliver(consumer, handler, timeout)
      else
        raise ArgumentError, "Not a valid subscription mode: #{mode}"
      end
      nil
    end

    def next(timeout=nil)
      setup!
      if timeout
        @internal_queue.poll((timeout * 1000).to_i, Concurrency::TimeUnit::MILLISECONDS)
      else
        @internal_queue.take
      end
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
            @internal_queue = nil
          end
        end
      end
    end

    private

    def setup!
      if @setup.compare_and_set(false, true)
        @setup_lock.lock do
          @queues = create_queues
          @internal_queue = create_internal_queue
          @subscriptions = create_subscriptions
          @subscriptions.each do |subscription|
            queue_consumer = QueueingConsumer.new(subscription.channel, @encoder, @internal_queue)
            subscription.start(queue_consumer)
          end
          @deliver.set(true)
        end
      end
    end

    def deliver(consumer, handler, timeout)
      begin
        headers, message = consumer.next(timeout)
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

    def create_internal_queue
      if @buffer_size || @prefetch
        Concurrency::ArrayBlockingQueue.new(@buffer_size || (@prefetch * @queues.size))
      else
        Concurrency::LinkedBlockingQueue.new
      end
    end

    def create_thread_pool(size)
      thread_factory = Concurrency::NamingDaemonThreadFactory.new('autobahn')
      Concurrency::Executors.new_fixed_thread_pool(size, thread_factory)
    end
  end

  class BatchConsumer
    def initialize(consumer, batcher)
      @consumer = consumer
      @batcher = batcher
      @buffer = Concurrency::LinkedBlockingQueue.new
    end

    def setup!
      @consumer.setup!
    end

    def subscribe(options={}, &handler)
      @consumer.subscribe(options.merge(:consumer => self), &handler)
    end

    def next(timeout=nil)
      pair = @buffer.poll
      unless pair
        pair = @consumer.next(timeout)
        if pair
          headers, messages = pair
          batch_headers = BatchHeaders.new(headers, messages.size)
          first_message = messages.shift
          messages.each do |message|
            @buffer.add([batch_headers, message])
          end
          return batch_headers, first_message
        end
      end
      return pair
    end

    def ack(consumer_tag, delivery_tag, options={})
      raise 'Not yet possible when batching'
      @consumer.ack(consumer_tag, delivery_tag, options)
    end

    def reject(consumer_tag, delivery_tag, options={})
      raise 'Not yet possible when batching'
      @consumer.reject(consumer_tag, delivery_tag, options)
    end

    def unsubscribe!
      @consumer.unsubscribe!
    end

    def disconnect!(*args)
      @consumer.disconnect!(*args)
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
    def initialize(channel, encoder, internal_queue)
      super(channel)
      @encoder = encoder
      @internal_queue = internal_queue
    end

    def deliver(*pair)
      pair[1] = @encoder.decode(pair[1])
      @internal_queue.put(pair)
    rescue Concurrency::InterruptedException => e
      $stderr.puts("INTERRUPTED!")
    end
  end
end
