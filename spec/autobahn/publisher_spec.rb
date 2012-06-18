require_relative '../spec_helper'


module Autobahn
  describe Publisher do
    describe '#publish' do
      let :simple_routing do
        {'stuff_queue_00' => {:routing_keys => %w[rk_00], :node => 'node_00'}}
      end

      let :exchange do
        double(:exchange)
      end

      let :connection do
        double(:connection).tap do |c|
          c.stub(:create_channel).and_return do
            double(:channel).tap do |ch|
              ch.stub(:exchange).and_return do
                exchange
              end
            end
          end
        end
      end

      let :connections do
        {'node_00' => connection}
      end

      let :encoder do
        double(:encoder).tap do |e|
          e.stub(:properties).and_return(:content_type => 'application/x-nonsense')
          e.stub(:encode).and_return(nil)
        end
      end

      before do
        @publisher = described_class.new('stuff', simple_routing, connections, encoder)
      end

      it 'publishes to the exchange' do
        exchange.should_receive(:publish)
        @publisher.publish('hello world')
      end

      it 'uses the encoder to encode messages' do
        encoder.stub(:encode).and_return('LULZ')
        exchange.should_receive(:publish).with('LULZ', anything)
        @publisher.publish('hello world')
      end

      it 'sets the content-type header to the value given by the encoder' do
        exchange.should_receive(:publish).with(anything, hash_including(:properties => {:content_type => 'application/x-nonsense'}))
        @publisher.publish('hello world')
      end
    end
  end
end