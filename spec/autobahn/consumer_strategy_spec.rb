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
    end

    context 'with a modulo subset' do
      it 'chooses the correct subset' do
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
end