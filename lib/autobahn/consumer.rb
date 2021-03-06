# encoding: utf-8

module Autobahn
  class Consumer
    def initialize(routing, connections, encoder_registry, options)
      @routing = routing
      @connections = connections
      @encoder_registry = encoder_registry
      @prefetch = options[:prefetch]
      @strategy = options[:strategy] || DefaultConsumerStrategy.new
      @buffer_size = options[:buffer_size]
      @preferred_decoder = options[:preferred_decoder]
      @fallback_decoder = options[:fallback_decoder]
      check_buffer_size!
      @demultiplexer = options[:demultiplexer] || BlockingQueueDemultiplexer.new(:buffer_size => @buffer_size)
      @logger = options[:logger] || NullLogger.new
      @setup = Concurrency::AtomicBoolean.new(false)
      @drain = Concurrency::AtomicBoolean.new(false)
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
      when :noop
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

    def drain!(timeout=30)
      @setup_lock.lock do
        unsubscribe!(timeout)
        unless @drain.get_and_set(true)
          logger.debug { 'Draining consumer' }
          if @deliver_pool
            unless Concurrency.shutdown_thread_pool!(@deliver_pool, timeout, 1)
              raise 'Could not shut down delivery thread pool'
            end
            @deliver_pool = nil
          end
          @demultiplexer = nil
          logger.info { 'Consumer drained' }
        end
      end
    end

    def unsubscribe!(timeout=30)
      @setup_lock.lock do
        if @subscriptions
          @subscriptions.each do |subscription|
            logger.debug { "Unsubscribing from #{subscription.queue_name}" }
            subscription.cancel
          end
          @subscriptions = nil
          logger.info do
            if @demultiplexer.respond_to?(:queued_message_count)
              "Consumer unsubscribed, #{@demultiplexer.queued_message_count} messages buffered"
            else
              'Consumer unsubscribed'
            end
          end
        end
      end
    end

    def disconnect!(timeout=30)
      @setup_lock.lock do
        drain!(timeout)
        if @queues
          logger.debug { 'Disconnecting consumer' }
          @queues.map(&:channel).each(&:close)
          @queues = nil
          logger.info { 'Consumer disconnected' }
        end
      end
    end

    private

    attr_reader :logger

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
          @logger.warn(%[No queues to subscribe to, transport system is empty]) if @routing.empty?
          @queues = create_queues
          @subscriptions = create_subscriptions
          @subscriptions.each do |subscription|
            logger.info { sprintf('Subscribing to %s with prefetch %d', subscription.queue_name, @prefetch || 0) }
            queue_consumer = QueueingConsumer.new(subscription.channel, @encoder_registry, @demultiplexer, :preferred_decoder => @preferred_decoder, :fallback_decoder => @fallback_decoder)
            subscription.start(queue_consumer)
          end
        end
      end
    end

    def deliver(handler, timeout)
      until @drain.get
        headers, message = self.next(timeout)
        handler.call(headers, message) if headers
      end
      while (headers, message = self.next(0))
        if headers
          handler.call(headers, message)
        else
          break
        end
      end
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

    def respond_to?(name)
      super unless @headers.respond_to?(name)
    end

    def method_missing(name, *args, &block)
      super unless @headers.respond_to?(name)
      @headers.send(name, *args, &block)
    end
  end

  class QueueingConsumer < HotBunnies::Queue::BaseConsumer
    def initialize(channel, encoder_registry, demultiplexer, options = {})
      super(channel)
      @encoder_registry = encoder_registry
      @demultiplexer = demultiplexer
      @preferred_decoder = options[:preferred_decoder]
      @fallback_decoder = options[:fallback_decoder]
      @preferred_decoder_content_type = @preferred_decoder && @preferred_decoder.properties[:content_type]
      @preferred_decoder_content_encoding = @preferred_decoder && @preferred_decoder.properties[:content_encoding]
    end

    def deliver(*pair)
      headers = pair[0]
      content_type = headers.content_type
      content_encoding = headers.content_encoding
      decoder = begin
        if @preferred_decoder && @preferred_decoder_content_type == content_type && @preferred_decoder_content_encoding == content_encoding
          @preferred_decoder
        elsif (decoder = @encoder_registry[content_type, :content_encoding => content_encoding])
          decoder
        elsif @fallback_decoder
          @fallback_decoder
        else
          raise UnknownEncodingError, "No available decoder for #{headers.content_type} and #{headers.content_encoding}"
        end
      end
      decoded_message = decoder.decode(pair[1])
      if decoder.encodes_batches? && decoded_message.is_a?(Array)
        if decoded_message.size == 0
          headers.ack
        elsif decoded_message.size == 1
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

    class UnknownEncodingError < StandardError; end
  end

  class BlockingQueueDemultiplexer
    def initialize(options={})
      if options[:buffer_size]
        @queue = Concurrency::ArrayBlockingQueue.new(options[:buffer_size])
      else
        @queue = Concurrency::LinkedBlockingQueue.new
      end
    end

    def queued_message_count
      @queue.size
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
