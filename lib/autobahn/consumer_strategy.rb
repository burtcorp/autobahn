# encoding: utf-8

module Autobahn
  class SubsetConsumerStrategy
    def initialize(consumer_index, num_consumers, mode=:range)
      @consumer_index, @num_consumers, @mode = consumer_index, num_consumers, mode
    end

    def subscribe?(index, total_count)
      if @mode == :modulo
        index % @num_consumers == @consumer_index
      else
        range_width = total_count/@num_consumers
        start_index = range_width * @consumer_index
        index >= start_index && index < start_index + range_width
      end
    end
  end

  class SingleConsumerStrategy
    def initialize(subscribe_index)
      @subscribe_index = subscribe_index
    end

    def subscribe?(index, total_count)
      index == @subscribe_index
    end
  end

  class DefaultConsumerStrategy
    def subscribe?(index, total_count)
      true
    end
  end
end