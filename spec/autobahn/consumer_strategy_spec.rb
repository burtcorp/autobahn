require_relative '../spec_helper'


module Autobahn
  describe SubsetConsumerStrategy do
    context 'with a range subset' do
      it 'chooses the correct subset' do
        strategy = described_class.new(1, 3)
        strategy.subscribe?(3, 12).should be_false
        strategy.subscribe?(4, 12).should be_true
        strategy.subscribe?(9, 12).should be_false
        strategy = described_class.new(2, 3)
        strategy.subscribe?(3, 12).should be_false
        strategy.subscribe?(4, 12).should be_false
        strategy.subscribe?(9, 12).should be_true
      end

      it 'includes every routing key exactly once' do
        routing_keys = 0..9
        strategies = 3.times.map { |consumer_index| described_class.new(consumer_index, 3) }
        routing_keys.each do |index|
          strategies.select { |strategy| strategy.subscribe?(index,routing_keys.count) }.size.should  == 1
        end
      end
    end

    context 'with a modulo subset' do
      it 'chooses every Nth' do
        strategy = described_class.new(1, 3, :modulo)
        strategy.subscribe?(3, 12).should be_false
        strategy.subscribe?(4, 12).should be_true
        strategy.subscribe?(9, 12).should be_false
        strategy = described_class.new(2, 3, :modulo)
        strategy.subscribe?(3, 12).should be_false
        strategy.subscribe?(4, 12).should be_false
        strategy.subscribe?(9, 12).should be_false
        strategy.subscribe?(8, 12).should be_true
      end
    end
  end
  describe SingleConsumerStrategy do
    it 'chooses only the Nth index' do
      strategy = described_class.new(1)
      strategy.subscribe?(0,3).should be_false
      strategy.subscribe?(1,3).should be_true
      strategy.subscribe?(2,3).should be_false
    end
    it 'disregards the total_count' do
      strategy = described_class.new(1)
      strategy.subscribe?(0,nil).should be_false
      strategy.subscribe?(1,nil).should be_true
      strategy.subscribe?(2,nil).should be_false
    end
  end
end
