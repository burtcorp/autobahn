# encoding: utf-8

module Autobahn
  class BatchOptions
    attr_reader :size, :timeout

    def initialize(options)
      @size = options[:size]
      @timeout = options[:timeout]
    end
  end
end