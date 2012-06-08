# encoding: utf-8

require_relative '../spec_helper'


describe Autobahn do
  let(:num_nodes) { 4 }
  let(:num_queues_per_node) { 3 }
  let(:num_queues) { num_nodes * num_queues_per_node }
  let(:base_port) { 6672 }
  let(:api_base_port) { 56672 }
  let(:exchange_name) { 'test_exchange' }
  let(:queue_prefix) { 'test_queue_' }
  let(:routing_key_prefix) { 'test_rk_' }
  let(:queue_names) { num_queues.times.map { |i| "#{queue_prefix}#{i.to_s.rjust(2, '0')}" } }
  let(:routing_keys) { num_queues.times.map { |i| "#{routing_key_prefix}#{i.to_s.rjust(2, '0')}" } }

  before :all do
    begin
      num_nodes.times do |i|
        connection = HotBunnies.connect(:port => base_port + i)
        channel = connection.create_channel
        exchange = channel.exchange(exchange_name, :type => :direct)
        num_queues_per_node.times do |j|
          queue_index = i * num_queues_per_node + j
          queue = channel.queue(queue_names[queue_index])
          queue.bind(exchange, :routing_key => routing_keys[queue_index])
        end
        channel.close
        connection.close
      end
    rescue java.net.ConnectException => e
      raise 'RabbitMQ test cluster not running, please run "spec/integration/bin/clusterctl start"'
    end
  end

  before :all do
    @transport_system = Autobahn::TransportSystem.new("http://localhost:#{api_base_port}/api", exchange_name)
  end

  after :all do
    @transport_system.disconnect! if @transport_system
  end

  before :all do
    @connection = HotBunnies.connect(:port => base_port)
    @exchange = @connection.create_channel.exchange(exchange_name, :passive => true)
    @queues = num_queues.times.map do |i|
      @connection.create_channel.queue(queue_names[i], :passive => true)
    end
  end

  after :all do
    @connection.close if @connection
  end

  before do
    @queues.each(&:purge)
  end

  describe 'Publishing to a transport system' do
    before do
      @publisher = @transport_system.publisher
    end

    after do
      @publisher.disconnect!
    end

    it 'publishes a message' do
      @publisher.publish('hello world')
      sleep(0.1) # allow time for for delivery
      message_count = @queues.reduce(0) { |n, q| n + q.status.first }
      message_count.should == 1
    end

    it 'publishes messages to random routing keys' do
      200.times { |i| @publisher.publish("hello world #{i}") }
      sleep(0.1) # allow time for for delivery
      message_counts = @queues.map { |q| q.status.first }
      message_counts.reduce(:+).should == 200
      message_counts.each { |c| c.should_not == 0 }
    end
  end

  describe 'Consuming a transport system' do
    before do
      @messages = (num_queues * 3).times.map { |i| "foo#{i}" }
      @messages.each_with_index do |msg, i|
        @exchange.publish(msg, :routing_key => routing_keys[i % num_queues])
      end
    end

    before do
      @consumer = @transport_system.consumer
    end

    after do
      @consumer.disconnect!
    end

    context 'when subscribed' do
      it 'delivers all available messages to the subscriber' do
        latch = Autobahn::Concurrency::CountDownLatch.new(@messages.size)
        @consumer.subscribe do |headers, message|
          headers.ack
          latch.count_down
        end
        latch.await(5, Autobahn::Concurrency::TimeUnit::SECONDS).should be_true
      end

      it 'subscribes to queues over connections to the node hosting the queue' do
        @consumer.subscribe { |headers, message| }
        queue_names.each do |queue_name|
          queue_info = @transport_system.cluster.queue("%2F/#{queue_name}")
          queue_node = queue_info['node']
          consumers = queue_info['consumer_details'].reduce([]) do |acc, consumer_details|
            channel_name = consumer_details['channel_details']['name']
            channel_info = @transport_system.cluster.channel(channel_name)
            acc << channel_info['node']
            acc
          end
          consumers.uniq.should == [queue_node]
        end
      end

      it 'uses the specified consumer strategy to decide which queues to subscribe to' do
        @consumer.disconnect!
        @consumer = @transport_system.consumer(:strategy => Autobahn::SubsetConsumerStrategy.new(2, 3))
        @consumer.subscribe { |headers, message| }
        subscribed_queues = queue_names.reduce([]) do |acc, queue_name|
          queue_info = @transport_system.cluster.queue("%2F/#{queue_name}")
          queue_node = queue_info['node']
          consumers = queue_info['consumer_details']
          acc << queue_name if consumers.size > 0
          acc
        end
        subscribed_queues.should == queue_names[8, 4]
      end
    end

    context 'using low level operations' do
      it 'exposes a raw interface for fetching the next message off of the local buffer' do
        messages = []
        messages << @consumer.next
        messages << @consumer.next
        messages.should have(2).items
      end

      it 'allows the client to ack with consumer and delivery tags' do
        headers, message = @consumer.next
        @consumer.ack(headers.consumer_tag, headers.delivery_tag)
        @consumer.disconnect!
        @queues.map { |q| q.status.first }.reduce(:+).should == 35
      end
    end
  end

  describe 'Transporting data through a transport system' do
    before do
      @publisher = @transport_system.publisher
      @consumer = @transport_system.consumer
    end

    before do
      @messages = 200.times.map { |i| "foo#{i}" }
    end

    after do
      @publisher.disconnect!
      @consumer.disconnect!
    end

    it 'transports messages from publisher to consumer' do
      latch = Autobahn::Concurrency::CountDownLatch.new(@messages.size)
      messages = []
      @consumer.subscribe do |headers, message|
        messages << message
        headers.ack
        latch.count_down
      end
      @messages.each { |msg| @publisher.publish(msg) }
      latch.await(5, Autobahn::Concurrency::TimeUnit::SECONDS).should be_true
      messages.sort.should == @messages.sort
    end
  end
end