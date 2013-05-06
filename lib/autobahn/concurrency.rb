# encoding: utf-8

require 'java'


module Autobahn
  module Concurrency
    java_import 'java.lang.Thread'
    java_import 'java.lang.InterruptedException'
    include_package 'java.util.concurrent'
    include_package 'java.util.concurrent.atomic'
    include_package 'java.util.concurrent.locks'

    class NamingDaemonThreadFactory
      include Concurrency::ThreadFactory

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

    class Lock
      def initialize
        @lock = Concurrency::ReentrantLock.new
      end

      def lock
        @lock.lock
        yield
      ensure
        @lock.unlock
      end
    end

    def self.shutdown_thread_pool!(tp, timeout=1, hard_timeout=30)
      tp.shutdown
      return true if tp.await_termination(timeout, TimeUnit::SECONDS)
      tp.shutdown_now
      return tp.await_termination(hard_timeout, TimeUnit::SECONDS)
    end
  end
end