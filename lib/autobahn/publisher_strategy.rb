# encoding: utf-8

require 'zlib'


module Autobahn
  class RandomPublisherStrategy
    def introspective?
      false
    end

    def select_routing_key(routing_keys, message)
      routing_keys.sample
    end
  end

  class PropertyGroupingPublisherStrategy
    def initialize(property, options={})
      @property = property
    end

    def introspective?
      true
    end

    def select_routing_key(routing_keys, message)
      h = Zlib.crc32(message[@property].to_s)
      routing_keys[h % routing_keys.size]
    end
  end
end
