require_relative '../spec_helper'


module Autobahn
  describe TransportSystem do
    let(:exchange_name) { 'stuff' }
    let(:api_uri) { 'http://rmq:55627/api' }

    let :cluster do
      double(:cluster).tap do |c|
        c.stub(:nodes).and_return([])
        c.stub(:bindings).and_return({})
        c.stub(:queues).and_return([])
      end
    end

    let :cluster_factory do
      double(:cluster_factory).tap do |cf|
        cf.stub(:new).and_return do
          cluster
        end
      end
    end

    let :connection do
      stub(:connection).tap do |c|
        c.stub(:create_channel) { stub(:channel).as_null_object }
        c.stub(:close)
      end
    end

    let :connection_factory do
      double(:connection_factory).tap do |cf|
        cf.stub(:connect).and_return(connection)
      end
    end

    describe '#name' do
      it 'returns the name of the exchange' do
        ts = described_class.new(api_uri, 'foobar')
        ts.name.should == 'foobar'
      end
    end

    describe '#size' do
      it 'returns the number of queues in the system' do
        cluster.stub(:queues).and_return([
          {'name' => 'queue0'},
          {'name' => 'queue1'},
          {'name' => 'queue2'}
        ])
        cluster.stub(:bindings).and_return([
          {'source' => exchange_name, 'destination_type' => 'queue', 'destination' => 'queue0'},
          {'source' => exchange_name, 'destination_type' => 'queue', 'destination' => 'queue1'},
          {'source' => exchange_name, 'destination_type' => 'queue', 'destination' => 'queue2'}
        ])
        transport_system = described_class.new(api_uri, exchange_name, :cluster_factory => cluster_factory)
        transport_system.size.should == 3
      end
    end

    describe '#publisher' do
      it 'returns a publisher' do
        transport_system = described_class.new(api_uri, exchange_name, :cluster_factory => cluster_factory)
        publisher = transport_system.publisher
        publisher.should_not be_nil
      end

      it 'raises an error if batching is enabled, but not a batch-compatible encoder (e.g. the default)' do
        transport_system = described_class.new(api_uri, exchange_name, :batch => {:size => 3}, :cluster_factory => cluster_factory)
        expect { transport_system.publisher }.to raise_error(ArgumentError)
      end

      it "overrides transport system's batch options" do
        Publisher.should_receive(:new).with(anything, anything, anything, anything, hash_including(:batch => {:size => 1337, :timeout => 10})).and_return(stub(:start! => nil))
        transport_system = described_class.new(api_uri, exchange_name, :batch => {:size => 3, :timeout => 10}, :cluster_factory => cluster_factory, :encoder => stub(:encodes_batches? => true))
        transport_system.publisher(:batch => { :size => 1337})
      end
    end

    describe '#connected?' do
      let :transport_system do
        described_class.new(api_uri, exchange_name, :cluster_factory => cluster_factory, :connection_factory => connection_factory)
      end

      before do
        cluster.stub(:exchanges).and_return([
          {'name' => exchange_name}
        ])
        cluster.stub(:queues).and_return([
          {'name' => 'queue0', 'node' => 'rmq@host0'},
          {'name' => 'queue1', 'node' => 'rmq@host1'},
          {'name' => 'queue2', 'node' => 'rmq@host2'}
        ])
        cluster.stub(:bindings).and_return([
          {'source' => exchange_name, 'destination_type' => 'queue', 'destination' => 'queue0'},
          {'source' => exchange_name, 'destination_type' => 'queue', 'destination' => 'queue1'},
          {'source' => exchange_name, 'destination_type' => 'queue', 'destination' => 'queue2'}
        ])
        cluster.stub(:nodes).and_return([
          {'name' => 'rmq@host0'},
          {'name' => 'rmq@host1'},
          {'name' => 'rmq@host2'},
        ])
        cluster.stub(:overview).and_return({
          'listeners' => [
            {'node' => 'rmq@host0', 'protocol' => 'amqp', 'port' => 5672},
            {'node' => 'rmq@host1', 'protocol' => 'amqp', 'port' => 5672},
            {'node' => 'rmq@host2', 'protocol' => 'amqp', 'port' => 5672}
          ]
        })
      end

      it 'is not connected initially' do
        transport_system.should_not be_connected
      end

      it 'is connected after a publisher has been created' do
        transport_system.publisher
        transport_system.should be_connected
      end

      it 'is connected after a consumer has been created' do
        transport_system.consumer
        transport_system.should be_connected
      end

      it 'is connected after being created' do
        cluster.stub(:exchanges).and_return([])
        cluster.stub(:queues).and_return([])
        cluster.stub(:bindings).and_return([])
        transport_system.create!
        transport_system.should be_connected
      end

      it 'is connected after being destroyed' do
        transport_system.destroy!
        transport_system.should be_connected
      end

      it 'is not connected after being disconnected' do
        transport_system.consumer
        transport_system.disconnect!
        transport_system.should_not be_connected
      end
    end
  end
end