require_relative '../spec_helper'


module Autobahn
  describe PropertyGroupingPublisherStrategy do
    let :strategy do
      described_class.new('foo')
    end

    let :routing_keys do
      %w[r0 r1 r2 r3]
    end

    describe '#select_routing_key' do
      it 'returns the same routing key for the same value' do
        rk1 = strategy.select_routing_key(routing_keys, {'foo' => 'bar'})
        rk2 = strategy.select_routing_key(routing_keys, {'foo' => 'bar'})
        rk1.should == rk2
      end

      it 'returns different routing keys for values that hash to different things' do
        rk1 = strategy.select_routing_key(routing_keys, {'foo' => 'bar'})
        rk2 = strategy.select_routing_key(routing_keys, {'foo' => 'baz'})
        rk1.should_not == rk2
      end

      it 'returns the same routing key for values that are hashed to the same routing key' do
        rk1 = strategy.select_routing_key(routing_keys, {'foo' => 'a'})
        rk2 = strategy.select_routing_key(routing_keys, {'foo' => 'c'})
        rk1.should == rk2
      end

      it 'handles numeric values' do
        rk1 = strategy.select_routing_key(routing_keys, {'foo' => 1})
        rk2 = strategy.select_routing_key(routing_keys, {'foo' => 3})
        rk1.should == rk2
      end

      it 'handles nil values' do
        rk1 = strategy.select_routing_key(routing_keys, {'foo' => nil})
        rk2 = strategy.select_routing_key(routing_keys, {'foo' => nil})
        rk1.should == rk2
      end
    end
  end
end