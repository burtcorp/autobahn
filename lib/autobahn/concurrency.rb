# encoding: utf-8

require 'java'


module Autobahn
  module Concurrency
    import 'java.lang.Thread'
    import 'java.util.concurrent.atomic.AtomicInteger'
    import 'java.util.concurrent.ThreadFactory'
    import 'java.util.concurrent.Executors'
    import 'java.util.concurrent.LinkedBlockingQueue'
    import 'java.util.concurrent.TimeUnit'
    import 'java.util.concurrent.CountDownLatch'

    class NamingDaemonThreadFactory
      include ThreadFactory

      def self.next_id
        @id ||= Concurrency::AtomicInteger.new
        @id.get_and_increment
      end

      def initialize(base_name)
        @base_name = base_name
      end

      def newThread(runnable)
        t = Concurrency::Thread.new(runnable)
        t.daemon = true
        t.name = "#@base_name-#{self.class.next_id}"
        t
      end
    end
  end
end