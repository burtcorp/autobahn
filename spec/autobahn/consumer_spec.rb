require_relative '../spec_helper'


module Autobahn
  describe Consumer do
    describe '#initialize' do
      stubs :encoder_registry

      let :routing do
        {
          'queue_00' => {:routing_keys => %w[rk_00 rk_01], :node => 'node_00'},
          'queue_01' => {:routing_keys => %w[rk_02 rk_03], :node => 'node_01'}
        }
      end

      let :connections do
        {
          'node_00' => stub,
          'node_01' => stub
        }
      end

      it 'raises an error if the buffer size is smaller than the number of routing keys' do
        options = {:buffer_size => 1}
        expect { described_class.new(routing, connections, encoder_registry, options) }.to raise_error
      end

      it 'takes the strategy into account before raising an error about the buffer size' do
        options = {:buffer_size => 1, :strategy => SubsetConsumerStrategy.new(1, 2)}
        expect { described_class.new(routing, connections, encoder_registry, options) }.not_to raise_error
      end
    end
  end
end