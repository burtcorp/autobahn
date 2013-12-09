require_relative '../spec_helper'


module Autobahn
  describe Publisher do
    let :simple_routing do
      {'stuff_queue_00' => {:routing_keys => %w[rk_00], :node => 'node_00'}}
    end

    let :complex_routing do
      {
        'stuff_queue_00' => {:routing_keys => %w[rk_00 rk_01], :node => 'node_00'},
        'stuff_queue_01' => {:routing_keys => %w[rk_02 rk_03], :node => 'node_01'}
      }
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

    let :single_connection do
      {'node_00' => connection}
    end

    let :multiple_connections do
      {'node_00' => connection, 'node_01' => connection}
    end

    let :encoder do
      double(:encoder).tap do |e|
        e.stub(:properties).and_return(:content_type => 'application/x-nonsense')
        e.stub(:encodes_batches?).and_return(false)
        e.stub(:encode).and_return(nil)
      end
    end

    describe '#start!' do
      it 'logs a warning when there are no routing keys' do
        logger = double(:logger)
        logger.stub(:warn)
        publisher = described_class.new('stuff', {}, single_connection, encoder, logger: logger)
        publisher.start!
        logger.should have_received(:warn).with(/No routing keys/)
      end
    end

    describe '#publish' do
      context 'when sending single messages' do
        before do
          @publisher = described_class.new('stuff', simple_routing, single_connection, encoder)
        end

        it 'publishes to the exchange' do
          exchange.should_receive(:publish)
          @publisher.publish('hello world')
        end

        it 'uses the encoder to encode messages' do
          encoder.stub(:encode).with('hello world').and_return('LULZ')
          exchange.should_receive(:publish).with('LULZ', anything)
          @publisher.publish('hello world')
        end

        it 'sets the content-type header to the value given by the encoder' do
          exchange.should_receive(:publish).with(anything, hash_including(:properties => {:content_type => 'application/x-nonsense'}))
          @publisher.publish('hello world')
        end

        it 'sets persistent flag for messages' do
          publisher = described_class.new('stuff', simple_routing, single_connection, encoder, {:publish => {:persistent => true}})
          exchange.should_receive(:publish).with(anything, hash_including(:properties => {:content_type => 'application/x-nonsense', :persistent => true}))
          publisher.publish('hello world')
        end

        it 'sets provides simple interface for setting persistent property' do
          publisher = described_class.new('stuff', simple_routing, single_connection, encoder, {:persistent => true})
          exchange.should_receive(:publish).with(anything, hash_including(:properties => {:content_type => 'application/x-nonsense', :persistent => true}))
          publisher.publish('hello world')
        end

        it 'does nothing when there are no routing keys' do
          exchange.stub(:publish)
          publisher = described_class.new('stuff', {}, single_connection, encoder)
          publisher.start!
          publisher.publish('hello world')
          exchange.should_not have_received(:publish)
        end
      end

      context 'when batching' do
        context 'with a non-introspective strategy' do
          before do
            options = {
              :batch => {:size => 3},
              :strategy => RandomPublisherStrategy.new # this is the default, but it doesn't hurt to be explicit
            }
            @publisher = described_class.new('stuff', complex_routing, multiple_connections, JsonEncoder.new, options)
          end

          it 'publishes on the third message' do
            exchange.should_receive(:publish).with(%([{"foo":1},{"foo":2},{"foo":3}]), anything)
            @publisher.publish({'foo' => 1})
            @publisher.publish({'foo' => 2})
            @publisher.publish({'foo' => 3})
          end
        end

        context 'with an introspective strategy' do
          before do
            options = {
              :batch => {:size => 3},
              :strategy => PropertyGroupingPublisherStrategy.new('foo')
            }
            @publisher = described_class.new('stuff', complex_routing, multiple_connections, JsonEncoder.new, options)
          end

          it 'publishes on the third message to the same routing key when using an introspective strategy' do
            exchange.should_receive(:publish).with(%([{"foo":1},{"foo":3},{"foo":8}]), hash_including(:routing_key => 'rk_03'))
            @publisher.publish({'foo' => 1})
            @publisher.publish({'foo' => 2})
            @publisher.publish({'foo' => 3})
            @publisher.publish({'foo' => 4})
            @publisher.publish({'foo' => 5})
            @publisher.publish({'foo' => 6})
            @publisher.publish({'foo' => 7})
            @publisher.publish({'foo' => 8})
          end
        end
      end
    end

    describe '#broadcast' do
      context 'simple routing' do
        it 'should publish to all routing keys' do
          exchange.should_receive(:publish).with(%([{"foo":1}]), hash_including(:routing_key => 'rk_00'))
          @publisher = described_class.new('stuff', simple_routing, single_connection, JsonEncoder.new)
          @publisher.broadcast({'foo' => 1})
        end
      end

      context 'complex routing' do
        it 'should publish to all routing keys' do
          %w[rk_00 rk_01 rk_02 rk_03].each do |rk|
            exchange.should_receive(:publish).with(%([{"foo":1}]), hash_including(:routing_key => rk))
          end
          @publisher = described_class.new('stuff', complex_routing, multiple_connections, JsonEncoder.new)
          @publisher.broadcast({'foo' => 1})
        end
      end

      context 'with no routing keys' do
        it 'does nothing' do
          exchange.stub(:publish)
          publisher = described_class.new('stuff', {}, single_connection, encoder)
          publisher.start!
          publisher.broadcast('hello world')
          exchange.should_not have_received(:publish)
        end
      end
    end

    describe '#disconnect!' do
      let :publisher do
        described_class.new('stuff', complex_routing, multiple_connections, JsonEncoder.new, logger: logger, batch: {size: 3})
      end

      let :logger do
        NullLogger.new
      end

      it 'flushes each routing key' do
        %w[rk_00 rk_01 rk_02 rk_03].each do |rk|
          exchange.should_receive(:publish).with(%([{"foo":1}]), hash_including(:routing_key => rk))
        end
        exchange.stub_chain(:channel, close: nil)
        publisher.broadcast({'foo' => 1})
        publisher.disconnect!
      end

      it 'flushes each routing key even on failure, and logs errors' do
        %w[rk_00 rk_01 rk_02 rk_03].each do |rk|
          exchange.should_receive(:publish).with(%([{"foo":1}]), hash_including(:routing_key => rk)).and_raise(TypeError)
        end
        logger.should_receive(:error).exactly(4).times
        exchange.stub_chain(:channel, close: nil)
        publisher.broadcast({'foo' => 1})
        publisher.disconnect!
      end
    end
  end
end