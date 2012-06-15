require_relative '../spec_helper'


module Autobahn
  describe TransportSystem do
    let(:exchange_name) { 'stuff' }
    let(:api_uri) { 'http://rmq:55627/api' }
    let(:cluster) do
      double(:cluster).tap do |c|
        c.stub(:bindings).and_return({})
        c.stub(:queues).and_return([])
      end
    end
    let(:cluster_factory) do
      double(:cluster_factory).tap do |cf|
        cf.stub(:new).and_return do
          cluster
        end
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
    end
  end
end