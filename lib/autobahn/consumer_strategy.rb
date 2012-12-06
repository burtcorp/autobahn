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
        div, mod = total_count.divmod(@num_consumers)
        start_index = div * @consumer_index + [@consumer_index, mod].min
        range_width = @consumer_index >= mod ? div : div + 1
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
