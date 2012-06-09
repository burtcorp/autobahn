# encoding: utf-8

module Autobahn
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

    def unsubscribe!
      return unless @subscribed
      @subscriptions.each(&:cancel)
      @subscriptions = nil
      @subscribed = false
    end

    def disconnect!
      return unless @running
      @running = false
      unsubscribe!
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
      thread_factory = Concurrency::NamingDaemonThreadFactory.new('autobahn')
      Concurrency::Executors.new_fixed_thread_pool(size, thread_factory)
    end
  end
end
