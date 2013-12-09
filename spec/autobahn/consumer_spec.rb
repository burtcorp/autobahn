require_relative '../spec_helper'


module Autobahn
  describe Consumer do
    describe '#initialize' do
      doubles :encoder_registry

      let :routing do
        {
          'queue_00' => {:routing_keys => %w[rk_00 rk_01], :node => 'node_00'},
          'queue_01' => {:routing_keys => %w[rk_02 rk_03], :node => 'node_01'}
        }
      end

      let :connections do
        {
          'node_00' => double,
          'node_01' => double
        }
      end

      describe '#initialize' do
        it 'raises an error if the buffer size is smaller than the number of routing keys' do
          options = {:buffer_size => 1}
          expect { described_class.new(routing, connections, encoder_registry, options) }.to raise_error
        end

        it 'takes the strategy into account before raising an error about the buffer size' do
          options = {:buffer_size => 1, :strategy => SubsetConsumerStrategy.new(1, 2)}
          expect { described_class.new(routing, connections, encoder_registry, options) }.not_to raise_error
        end
      end

      describe '#subscribe' do
        it 'logs a warning when the transport system is empty' do
          logger = double(:logger)
          logger.stub(:warn)
          consumer = described_class.new({}, connections, encoder_registry, logger: logger)
          consumer.subscribe { }
          logger.should have_received(:warn).with(/transport system is empty/)
        end
      end
    end
  end
end