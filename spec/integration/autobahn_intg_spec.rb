# encoding: utf-8

require_relative '../spec_helper'


describe Autobahn do
  HOSTNAME = `hostname -s`.chomp
  NUM_NODES = 4
  NUM_QUEUES_PER_NODE = 3
  NUM_QUEUES = NUM_NODES * NUM_QUEUES_PER_NODE
  BASE_PORT = 6672
  API_BASE_PORT = 16672
  EXCHANGE_NAME = 'test_exchange'.freeze
  QUEUE_NAMES = Array.new(NUM_QUEUES) { |i| "test_queue_#{i.to_s.rjust(2, '0')}".freeze }.freeze
  ROUTING_KEYS = Array.new(NUM_QUEUES) { |i| "test_rk_#{i.to_s.rjust(2, '0')}".freeze }.freeze

  def counting_down(n, options={})
    latch = Autobahn::Concurrency::CountDownLatch.new(n)
    yield latch
    latch.should_not time_out.within(options[:timeout])
  end

  def await_delivery
    sleep(0.5)
  end

  def create_transport_system(name, options={})
    logger = Logger.new(STDERR)
    logger.level = Logger.const_get((ENV['LOG_LEVEL'] || 'FATAL').upcase)
    logger.progname = name
    Autobahn.transport_system("http://#{HOSTNAME}:#{API_BASE_PORT}/api", name, options.merge(:logger => logger))
  end

  before :all do
    begin
      NUM_NODES.times do |i|
        connection = HotBunnies.connect(:host => HOSTNAME, :port => BASE_PORT + i)
        channel = connection.create_channel
        exchange = channel.exchange(EXCHANGE_NAME, :type => :direct)
        NUM_QUEUES_PER_NODE.times do |j|
          queue_index = i * NUM_QUEUES_PER_NODE + j
          queue = channel.queue(QUEUE_NAMES[queue_index])
          queue.bind(exchange, :routing_key => ROUTING_KEYS[queue_index])
        end
        channel.close
        connection.close
      end
    rescue java.net.ConnectException => e
      raise 'RabbitMQ test cluster not running, please run "spec/integration/bin/clusterctl start"'
    end
  end

  before :all do
    @transport_system = create_transport_system(EXCHANGE_NAME)
  end

  after :all do
    @transport_system.disconnect! if @transport_system
  end

  before :all do
    @connection = HotBunnies.connect(:port => BASE_PORT)
    @exchange = @connection.create_channel.exchange(EXCHANGE_NAME, :passive => true)
    @queues = NUM_QUEUES.times.map do |i|
      @connection.create_channel.queue(QUEUE_NAMES[i], :passive => true)
    end
  end

  after :all do
    @connection.close if @connection
  end

  before do
    @queues.each(&:purge)
  end

  describe 'Publishing to a transport system' do
    context 'when publishing' do
      before do
        @publisher = @transport_system.publisher
      end

      after do
        @publisher.disconnect!
      end

      it 'publishes a message' do
        @publisher.publish('hello world')
        await_delivery
        message_count = @queues.reduce(0) { |n, q| n + q.status.first }
        message_count.should == 1
      end

      it 'publishes messages to random routing keys' do
        200.times { |i| @publisher.publish("hello world #{i}") }
        await_delivery
        message_counts = @queues.map { |q| q.status.first }
        message_counts.reduce(:+).should == 200
        message_counts.each { |c| c.should_not == 0 }
      end

      it 'publishes to an empty transport system' do
        ts = create_transport_system('empty')
        ts.create!(queues_per_node: 0)
        begin
          publisher = ts.publisher
          publisher.publish('hello world')
        ensure
          ts.destroy!
          ts.disconnect!
        end
      end

      it 'publishes persistent messages' do
        @publisher.publish('hello world')
        await_delivery
        headers = @queues.map { |q| h, _ = q.get; h }.compact
        headers.first.properties.delivery_mode.should == 2
      end
    end

    context 'with encoded messages' do
      before do
        @encoded_transport_system = create_transport_system(EXCHANGE_NAME, :encoder => Autobahn::JsonEncoder.new)
      end

      after do
        @encoded_transport_system.disconnect! if @encoded_transport_system
      end

      it 'uses the provided encoder to encode messages' do
        publisher = @encoded_transport_system.publisher
        publisher.publish({'hello' => 'world'})
        await_delivery
        @queues.map { |q| h, m = q.get; m }.compact.first.should == '[{"hello":"world"}]'
      end
    end

    context 'with compressed messages' do
      before do
        @encoder = Autobahn::GzipEncoder.new(Autobahn::JsonEncoder.new)
        options = {:encoder => @encoder}
        @compressed_transport_system = create_transport_system(EXCHANGE_NAME, options)
        publisher = @compressed_transport_system.publisher
        publisher.publish({'hello' => 'world'})
        await_delivery
      end

      after do
        @compressed_transport_system.disconnect! if @compressed_transport_system
      end

      it 'uses the provided encoder to also compress messages' do
        actual_message = @queues.map { |q| h, m = q.get; m }.compact.first
        actual_message.should == @encoder.encode([{'hello' => 'world'}])
      end

      it 'sets the content-encoding header' do
        @queues.map { |q| h, m = q.get; h.properties.content_encoding if h }.compact.first.should == 'gzip'
      end

      it 'sets the content-type header' do
        @queues.map { |q| h, m = q.get; h.properties.content_type if h }.compact.first.should == 'application/json'
      end
    end

    context 'with custom strategies' do
      it 'uses the provided strategy to select routing keys' do
        strategy = Autobahn::PropertyGroupingPublisherStrategy.new('genus')
        publisher = @transport_system.publisher(:strategy => strategy, :encoder => Autobahn::JsonEncoder.new)
        publisher.publish('name' => 'Common chimpanzee',      'species' => 'Pan troglodytes',  'genus' => 'Pan')
        publisher.publish('name' => 'Bonobo',                 'species' => 'Pan paniscus',     'genus' => 'Pan')
        publisher.publish('name' => 'Common squirrel monkey', 'species' => 'Saimiri sciureus', 'genus' => 'Saimiri')
        publisher.publish('name' => 'Rhesus macaque',         'species' => 'Macaca mulatta',   'genus' => 'Macaca')
        publisher.publish('name' => 'Sumatran orangutan',     'species' => 'Pongo abelii',     'genus' => 'Pongo')
        await_delivery
        queue_sizes = @queues.map { |q| q.status.first }
        queue_sizes.should == [0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 2]
      end
    end

    context 'with batched messages' do
      before do
        @encoder = Autobahn::JsonEncoder.new
        @batch_size = 3
        @batch_timeout = 1
        options = {
          :encoder => @encoder,
          :batch => {
            :size => @batch_size,
            :timeout => @batch_timeout
          }
        }
        @batching_transport_system = create_transport_system(EXCHANGE_NAME, options)
        @publisher = @batching_transport_system.publisher
      end

      after do
        @batching_transport_system.disconnect! if @batching_transport_system
      end

      it 'it packs messages into batches' do
        @publisher.publish('hello' => 'world')
        @publisher.publish('foo' => 'bar')
        @publisher.publish('abc' => '123')
        @publisher.publish('xyz' => '999')
        await_delivery
        message = @queues.map { |q| h, m = q.get; m }.compact.first
        @encoder.decode(message).should == [{'hello' => 'world'}, {'foo' => 'bar'}, {'abc' => '123'}]
      end

      it 'sets the correct headers' do
        @publisher.publish('hello' => 'world')
        @publisher.publish('foo' => 'bar')
        @publisher.publish('abc' => '123')
        @publisher.publish('xyz' => '999')
        await_delivery
        headers = @queues.map { |q| h, m = q.get; h }.compact.first
        headers.content_type.should == 'application/json'
      end

      it 'sends a batch after a timeout, even if it is not full' do
        @publisher.publish('hello' => 'world')
        @publisher.publish('foo' => 'bar')
        sleep(@batch_timeout * 2)
        await_delivery
        message = @queues.map { |q| h, m = q.get; m }.compact.first
        @encoder.decode(message).should == [{'hello' => 'world'}, {'foo' => 'bar'}]
      end

      it 'respects routing when sending batches' do
        publisher = @batching_transport_system.publisher(:strategy => Autobahn::PropertyGroupingPublisherStrategy.new('id'))
        ('A'..'Z').each do |id|
          publisher.publish('id' => id)
        end
        h1, m1 = @queues[11].get
        @encoder.decode(m1).should == [{'id' => 'A'}, {'id' => 'C'}, {'id' => 'H'}]
      end

      it 'can flush batches on demand' do
        @publisher.publish('hello' => 'world')
        @publisher.publish('foo' => 'bar')
        @publisher.flush!
        message = @queues.map { |q| h, m = q.get; m }.compact.first
        @encoder.decode(message).should == [{'hello' => 'world'}, {'foo' => 'bar'}]
      end
    end
  end

  describe 'Consuming a transport system' do
    let :messages do
      (NUM_QUEUES * 3).times.map { |i| "foo#{i}" }
    end

    before do
      @consumer = @transport_system.consumer
    end

    after do
      @consumer.disconnect!
    end

    context 'when subscribed' do
      before do
        messages.each_with_index do |msg, i|
          @exchange.publish(msg, :routing_key => ROUTING_KEYS[i % NUM_QUEUES], :properties => {:content_type => 'application/octet-stream'})
        end
      end

      it 'delivers all available messages to the subscriber' do
        counting_down(messages.size) do |latch|
          @consumer.subscribe do |headers, message|
            headers.ack
            latch.count_down
          end
        end
      end

      it 'subscribes to queues over connections to the node hosting the queue' do
        @consumer.subscribe { |headers, message| }
        QUEUE_NAMES.each do |queue_name|
          queue_info = @transport_system.cluster.queue("%2F/#{URI.encode(queue_name)}")
          queue_node = queue_info['node']
          consumers = queue_info['consumer_details'].reduce([]) do |acc, consumer_details|
            channel_name = consumer_details['channel_details']['name']
            channel_info = @transport_system.cluster.channel(URI.encode(channel_name))
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
        subscribed_queues = QUEUE_NAMES.reduce([]) do |acc, queue_name|
          queue_info = @transport_system.cluster.queue("%2F/#{URI.encode(queue_name)}")
          queue_node = queue_info['node']
          consumers = queue_info['consumer_details']
          acc << queue_name if consumers.size > 0
          acc
        end
        subscribed_queues.should == QUEUE_NAMES[8, 4]
      end

      it 'does not receive new messages after it has been unsubscribed' do
        counting_down(messages.size) do |latch|
          message_count = 0
          @consumer.subscribe do |headers, message|
            message_count += 1
            headers.ack
            latch.count_down
          end
        end
        @consumer.unsubscribe!
        @exchange.publish('foo', :routing_key => ROUTING_KEYS.sample, :properties => {:content_type => 'application/octet-stream'})
        await_delivery
        @queues.map { |q| q.status.first }.reduce(:+).should == 1
      end

      it 'waits for the current delivery to finish when draining' do
        deliver_semaphore = Autobahn::Concurrency::Semaphore.new(0)
        semaphore = Autobahn::Concurrency::Semaphore.new(0)
        demultiplexer = double(:demultiplexer)
        demultiplexer.stub(:put)
        demultiplexer.stub(:take) do |t|
          semaphore.release
          if deliver_semaphore.try_acquire(100, Autobahn::Concurrency::TimeUnit::MILLISECONDS)
            sleep 0.1
            [double(:headers), double(:message)]
          end
        end
        @consumer.disconnect!
        @consumer = @transport_system.consumer(demultiplexer: demultiplexer)
        sink = double(:sink, consume: nil)
        @consumer.subscribe(&sink.method(:consume))
        semaphore.acquire
        t = Thread.start { semaphore.release; @consumer.drain! }
        semaphore.acquire
        deliver_semaphore.release(10)
        t.join
        sink.should have_received(:consume).exactly(10).times
      end

      it 'interrupts slow-running delivery handler(s) when draining' do
        semaphore = Autobahn::Concurrency::Semaphore.new(0)
        interrupted = Autobahn::Concurrency::AtomicBoolean.new(false)
        @consumer.subscribe do |headers, message|
          begin
            semaphore.acquire
          rescue java.lang.InterruptedException
            interrupted.set(true)
          end
        end
        @consumer.drain!(1) rescue nil
        interrupted.get.should be_true
      end

      it 'stops subscription thread(s) when draining' do
        threads = []
        mutex = Mutex.new
        @consumer.subscribe do |headers, message|
          mutex.synchronize do
            threads << Thread.current
          end
        end
        @consumer.drain!
        threads.each do |thread|
          thread.should_not be_alive
        end
      end
    end

    context 'using low level operations' do
      before do
        messages.each_with_index do |msg, i|
          @exchange.publish(msg, :routing_key => ROUTING_KEYS[i % NUM_QUEUES], :properties => {:content_type => 'application/octet-stream'})
        end
        await_delivery
      end

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

      it 'allows overriding of the local buffer, for very low level consuming' do
        demultiplexer = SubdividingDemultiplexer.new(3)
        @consumer.disconnect!
        @consumer = @transport_system.consumer(:demultiplexer => demultiplexer)
        @consumer.subscribe(:mode => :noop)
        _, msg0 = demultiplexer.take_next(0)
        _, msg1 = demultiplexer.take_next(1)
        _, msg2 = demultiplexer.take_next(2)
        [msg0, msg1, msg2].compact.should_not be_empty
      end
    end

    context 'with encoded messages' do
      before do
        @consumer.disconnect!
        @encoded_transport_system = create_transport_system(EXCHANGE_NAME)
      end

      after do
        @encoded_transport_system.disconnect! if @encoded_transport_system
      end

      it 'auto discovers the encoding based on the content type header' do
        @exchange.publish('{"hello":"world"}', :routing_key => ROUTING_KEYS.sample, :properties => {:content_type => 'application/json'})
        await_delivery
        @consumer = @encoded_transport_system.consumer
        h, m = @consumer.next
        m.should == {'hello' => 'world'}
      end

      it 'uses preferred_decoder when possible' do
        @exchange.publish('message', :routing_key => ROUTING_KEYS.sample, :properties =>  {:content_type => 'application/spec', :content_encoding => 'spec'})
        await_delivery
        preferred_decoder = double('Encoder')
        preferred_decoder.stub(:properties => {:content_type => 'application/spec', :content_encoding => 'spec'},:encodes_batches? => false)
        preferred_decoder.should_receive(:decode)
        @consumer = @encoded_transport_system.consumer(:preferred_decoder => preferred_decoder)
        @consumer.next
      end

      it "doesn't use preferred_decoder when content-type/encoding don't match" do
        @exchange.publish('{"hello":"world"}', :routing_key => ROUTING_KEYS.sample, :properties =>  {:content_type => 'application/json'})
        await_delivery
        preferred_decoder = double('Encoder')
        preferred_decoder.stub(:properties => {:content_type => 'application/spec', :content_encoding => 'spec'},:encodes_batches? => false)
        preferred_decoder.should_not_receive(:decode)
        @consumer = @encoded_transport_system.consumer(:preferred_decoder => preferred_decoder)
        @consumer.next
      end

      it 'still supports :encoder option' do
        @exchange.publish('message', :routing_key => ROUTING_KEYS.sample, :properties =>  {:content_type => 'application/spec', :content_encoding => 'spec'})
        await_delivery
        preferred_decoder = double('Encoder')
        preferred_decoder.stub(:properties => {:content_type => 'application/spec', :content_encoding => 'spec'},:encodes_batches? => false)
        preferred_decoder.should_receive(:decode)
        @consumer = @encoded_transport_system.consumer(:encoder => preferred_decoder)
        @consumer.next
      end

      it 'uses fallback_decoder for unknown encodings' do
        @exchange.publish('message', :routing_key => ROUTING_KEYS.sample, :properties =>  {:content_type => 'application/spec', :content_encoding => 'spec'})
        await_delivery
        fallback_decoder = double('Encoder')
        fallback_decoder.stub(:properties => {},:encodes_batches? => false)
        fallback_decoder.should_receive(:decode)
        @consumer = @encoded_transport_system.consumer(:fallback_decoder => fallback_decoder)
        @consumer.next
      end

      it  "doesn't use fallback_decoder when encoder for content-type/encoding exist" do
        @exchange.publish('{"hello":"world"}', :routing_key => ROUTING_KEYS.sample, :properties =>  {:content_type => 'application/json'})
        await_delivery
        fallback_decoder = double('Encoder')
        fallback_decoder.stub(:properties => {},:encodes_batches? => false)
        fallback_decoder.should_not_receive(:decode)
        @consumer = @encoded_transport_system.consumer(:fallback_decoder => fallback_decoder)
        @consumer.next
      end
    end

    context 'with compressed messages' do
      before do
        @consumer.disconnect!
        @compressed_transport_system = create_transport_system(EXCHANGE_NAME)
        compressed_message = [31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 171, 86, 202, 72, 205, 201, 201, 87, 178, 82, 42, 207, 47, 202, 73, 81, 170, 5, 0, 209, 65, 9, 216, 17, 0, 0, 0].pack('C*')
        @exchange.publish(compressed_message, :routing_key => ROUTING_KEYS.sample, :properties => {:content_type => 'application/json', :content_encoding => 'gzip'})
        await_delivery
      end

      after do
        @compressed_transport_system.disconnect! if @compressed_transport_system
      end

      it 'auto discovers the compression based on the content encoding property' do
        @consumer = @compressed_transport_system.consumer
        h, m = @consumer.next(5)
        m.should == {'hello' => 'world'}
      end
    end

    context 'with batched messages' do
      before do
        encoder = Autobahn::JsonEncoder.new
        @batch_size = messages.size/2
        options = {
          :batch => {:size => @batch_size, :timeout => 2}
        }
        @batching_transport_system = create_transport_system(EXCHANGE_NAME, options)
        messages.each_slice(@batch_size) do |batch|
          @exchange.publish(encoder.encode(batch), :routing_key => ROUTING_KEYS.sample, :properties => encoder.properties)
        end
        @latch = Autobahn::Concurrency::CountDownLatch.new(messages.size)
        @consumer.disconnect!
        @consumer = @batching_transport_system.consumer
      end

      after do
        @batching_transport_system.disconnect! if @batching_transport_system
      end

      it 'unpacks batches' do
        received_messages = []
        counting_down(messages.size) do |latch|
          @consumer.subscribe do |headers, message|
            received_messages << message
            headers.ack
            latch.count_down
          end
        end
        received_messages.sort.should == messages.sort
      end

      it 'acks the batch, but only once' do
        counting_down(messages.size) do |latch|
          @consumer.subscribe do |headers, message|
            headers.ack
            latch.count_down
          end
        end
        @queues.map { |q| c, _ = q.status; c }.reduce(:+).should == 0
      end
    end
  end

  describe 'Transporting data through a transport system' do
    it 'transports messages from publisher to consumer' do
      publisher = @transport_system.publisher
      consumer = @transport_system.consumer
      messages = 200.times.map { |i| "foo#{i}" }
      recv_messages = []
      begin
        counting_down(messages.size) do |latch|
          consumer.subscribe do |headers, message|
            recv_messages << message
            headers.ack
            latch.count_down
          end
          messages.each { |msg| publisher.publish(msg) }
        end
        recv_messages.sort.should == messages.sort
      ensure
        publisher.disconnect!
        consumer.disconnect!
      end
    end

    def transport_messages!(options)
      transport_system = create_transport_system(EXCHANGE_NAME, options)
      publisher = transport_system.publisher
      consumer = transport_system.consumer
      messages = 200.times.map { |i| {'foo' => "bar#{i}"} }
      recv_messages = []
      begin
        counting_down(messages.size) do |latch|
          consumer.subscribe do |headers, message|
            recv_messages << message
            headers.ack
            latch.count_down
          end
          messages.each { |msg| publisher.publish(msg) }
        end
        recv_messages.sort_by { |m| m['foo'] }.should == messages.sort_by { |m| m['foo'] }
      ensure
        transport_system.disconnect! if transport_system
      end
    end

    it 'transports messages encoded' do
      transport_messages!(:encoder => Autobahn::JsonEncoder.new)
    end

    it 'transports messages in batches' do
      transport_messages!(
        :encoder => Autobahn::JsonEncoder.new,
        :batch => {:size => 10, :timeout => 1}
      )
    end

    it 'transports messages compressed' do
      transport_messages!(
        :encoder => Autobahn::GzipEncoder.new(Autobahn::JsonEncoder.new),
        :batch => {:size => 10, :timeout => 1}
      )
    end
  end

  describe 'Creating a transport system' do
    context 'when inquiring about existence' do
      it 'can tell that a transport system exists' do
        ts = create_transport_system(EXCHANGE_NAME)
        ts.exists?.should be_true
      end

      it 'can tell that a transport system does not exist' do
        ts = create_transport_system('bogus_name')
        ts.exists?.should be_false
      end
    end

    context 'when creating a transport system' do
      before do
        @stuff_ts = create_transport_system('stuff')
        @stuff_ts.create!
      end

      after do
        @stuff_ts.disconnect!
        @connection.create_channel.exchange_delete('stuff') rescue nil
        @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('stuff_') }.each { |q| @connection.create_channel.queue_delete(q['name']) rescue nil }
      end

      it 'creates the exchange' do
        @stuff_ts.cluster.exchanges.find { |e| e['name'] == 'stuff' }.should_not be_nil
      end

      it 'creates the queues' do
        @stuff_ts.cluster.queues.map { |q| q['name'] }.select { |n| n.start_with?('stuff_') }.should == %w[stuff_00 stuff_01 stuff_02 stuff_03]
      end

      it 'binds the queues to the exchange' do
        bindings = @stuff_ts.cluster.bindings.select { |b| b['source'] == 'stuff' && b['destination_type'] == 'queue' && b['destination'].start_with?('stuff_') }
        binding_mappings = Hash[bindings.map { |b| [b['routing_key'], b['destination']] }]
        binding_mappings.should == {'stuff_00' => 'stuff_00', 'stuff_01' => 'stuff_01', 'stuff_02' => 'stuff_02', 'stuff_03' => 'stuff_03'}
      end

      it 'distributes the queues evenly over the nodes' do
        queues = @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('stuff_') }
        queues.group_by { |q| q['node'] }.map { |g, qs| qs.size }.should == [1, 1, 1, 1]
      end

      it 'creates a durable exchange' do
        exchange = @stuff_ts.cluster.exchanges.find { |e| e['name'] == 'stuff' }
        exchange['durable'].should be_true
      end

      it 'create durable queues' do
        queues = @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('stuff_') }
        queues.map { |q| q['durable'] }.uniq.should == [true]
      end

      it 'does not create HA queues by default' do
        queues = @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('stuff_') }
        queues.map { |q| q['arguments'] }.uniq.should == [{}]
      end
    end

    context 'when creating a transport system with HA' do
      it 'creates HA mirrored on all nodes when :ha is :all' do
        pending
        ha_ts = create_transport_system('stuff')
        begin
          ha_ts.create!(:ha => :all)
          queues = ha_ts.cluster.queues.select { |q| q['name'].start_with?('xyz') }
          queues.map { |q| q['arguments'] }.uniq.should == [{'x-ha-policy' => 'all'}]
        ensure
          ha_ts.disconnect!
          @connection.create_channel.exchange_delete('stuff') rescue nil
          ha_ts.cluster.queues.select { |q| q['name'].start_with?('stuff_') }.each { |q| @connection.create_channel.queue_delete(q['name']) rescue nil }
        end
      end

      it 'creates HA mirrored on N nodes when :ha is N' do
        pending
        ha_ts = create_transport_system('stuff')
        begin
          ha_ts.create!(:ha => 2)
          queues = ha_ts.cluster.queues.select { |q| q['name'].start_with?('xyz') }
          queues.map { |q| q['arguments'] }.uniq.should == [{'x-ha-policy' => 'nodes'}]
          p queues
        ensure
          ha_ts.disconnect!
          @connection.create_channel.exchange_delete('stuff') rescue nil
          ha_ts.cluster.queues.select { |q| q['name'].start_with?('stuff_') }.each { |q| @connection.create_channel.queue_delete(q['name']) rescue nil }
        end
      end
    end

    context 'when creating a transport system with options' do
      before do
        @stuff_ts = create_transport_system('stuff')
        @stuff_ts.create!(
          :queues_per_node => 2, 
          :routing_keys_per_queue => 4,
          :queue_prefix => 'xyz',
          :routing_key_prefix => 'blipp_',
          :durable => false
        )
      end

      after do
        @stuff_ts.disconnect!
        @connection.create_channel.exchange_delete('stuff') rescue nil
        @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('xyz') }.each { |q| @connection.create_channel.queue_delete(q['name']) rescue nil }
      end

      it 'creates queues with names with the specified prefix' do
        @stuff_ts.cluster.queues.map { |q| q['name'] }.select { |n| n.start_with?('xyz') }.should == %w[xyz00 xyz01 xyz02 xyz03 xyz04 xyz05 xyz06 xyz07]
      end

      it 'creates the specified number of queues per node' do
        queues = @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('xyz') }
        queues.group_by { |q| q['node'] }.map { |g, qs| qs.size }.should == [2, 2, 2, 2]
      end

      it 'binds the queues using routing keys with the specified prefix' do
        bindings = @stuff_ts.cluster.bindings.select { |b| b['source'] == 'stuff' && b['destination_type'] == 'queue' && b['destination'].start_with?('xyz') }
        binding_mappings = Hash[bindings.map { |b| [b['routing_key'], b['destination']] }]
        binding_mappings.should == {
          'blipp_00' => 'xyz00', 'blipp_01' => 'xyz00', 'blipp_02' => 'xyz00', 'blipp_03' => 'xyz00',
          'blipp_04' => 'xyz01', 'blipp_05' => 'xyz01', 'blipp_06' => 'xyz01', 'blipp_07' => 'xyz01',
          'blipp_08' => 'xyz02', 'blipp_09' => 'xyz02', 'blipp_10' => 'xyz02', 'blipp_11' => 'xyz02',
          'blipp_12' => 'xyz03', 'blipp_13' => 'xyz03', 'blipp_14' => 'xyz03', 'blipp_15' => 'xyz03',
          'blipp_16' => 'xyz04', 'blipp_17' => 'xyz04', 'blipp_18' => 'xyz04', 'blipp_19' => 'xyz04',
          'blipp_20' => 'xyz05', 'blipp_21' => 'xyz05', 'blipp_22' => 'xyz05', 'blipp_23' => 'xyz05',
          'blipp_24' => 'xyz06', 'blipp_25' => 'xyz06', 'blipp_26' => 'xyz06', 'blipp_27' => 'xyz06',
          'blipp_28' => 'xyz07', 'blipp_29' => 'xyz07', 'blipp_30' => 'xyz07', 'blipp_31' => 'xyz07',
        }
      end

      it 'zero fills with enough zeroes' do
        ts = create_transport_system('more_stuff')
        ts.create!(:queues_per_node => 40)
        begin
          queue_names = ts.cluster.queues.map { |q| q['name'] }.select { |n| n.start_with?('more_stuff_') }
          rk_names = ts.cluster.bindings.map { |q| q['routing_key'] }.select { |n| n.start_with?('more_stuff_') }
          queue_names.should include('more_stuff_000')
          queue_names.should include('more_stuff_099')
          queue_names.should include('more_stuff_101')
          rk_names.should include('more_stuff_001')
          rk_names.should include('more_stuff_088')
          rk_names.should include('more_stuff_159')
        ensure
          ts.disconnect!
          @connection.create_channel.exchange_delete('more_stuff') rescue nil
          ts.cluster.queues.select { |q| q['name'].start_with?('more_stuff_') }.each { |q| @connection.create_channel.queue_delete(q['name']) rescue nil }
        end
      end

      it 'creates a non-durable exchange when :durable is false' do
        exchange = @stuff_ts.cluster.exchanges.find { |e| e['name'] == 'stuff' }
        exchange['durable'].should be_false
      end

      it 'create non-durable queues when :durable is false' do
        queues = @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('xyz') }
        queues.map { |q| q['durable'] }.uniq.should == [false]
      end

      it 'creates no queues and binds no routing keys when :queues_per_node is zero' do
        ts = create_transport_system('empty')
        ts.create!(queues_per_node: 0)
        begin
          queues = ts.cluster.queues.select { |q| q['name'].start_with?('empty') }
          queues.should be_empty
          routing_keys = ts.cluster.bindings.select { |rk| rk['routing_key'].start_with?('empty') }
          routing_keys.should be_empty
        ensure
          ts.disconnect!
          @connection.create_channel.exchange_delete('empty') rescue nil
        end
      end
    end
  end

  describe 'Destroying a transport system' do
    before do
      @stuff_ts = create_transport_system('stuff')
      @stuff_ts.create!
      @stuff_ts.destroy!
    end

    after do
      @stuff_ts.disconnect!
    end

    it 'removes all queues' do
      queues = @stuff_ts.cluster.queues.select { |q| q['name'].start_with?('stuff') }
      queues.should be_empty
    end

    it 'removes the exchange' do
      exchange = @stuff_ts.cluster.exchanges.find { |e| e['name'] == 'stuff' }
      exchange.should be_nil
    end

    it 'does nothing if the system already does not exist' do
      expect { @stuff_ts.destroy! }.to_not raise_error
    end
  end
end

class SubdividingDemultiplexer
  def initialize(size)
    @buffers = size.times.map { Autobahn::Concurrency::ArrayBlockingQueue.new(100) }
  end

  def put(pair)
    @buffers[pair.last.hash % @buffers.size].put(pair)
  end

  def take_next(n)
    @buffers[n].poll
  end
end