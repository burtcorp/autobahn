# encoding: utf-8

require 'java'


module Autobahn
  module Concurrency
    java_import 'java.lang.Thread'
    java_import 'java.lang.InterruptedException'
    java_import 'java.util.concurrent.atomic.AtomicInteger'
    java_import 'java.util.concurrent.atomic.AtomicBoolean'
    java_import 'java.util.concurrent.ThreadFactory'
    java_import 'java.util.concurrent.Executors'
    java_import 'java.util.concurrent.LinkedBlockingQueue'
    java_import 'java.util.concurrent.LinkedBlockingDeque'
    java_import 'java.util.concurrent.ArrayBlockingQueue'
    java_import 'java.util.concurrent.TimeUnit'
    java_import 'java.util.concurrent.CountDownLatch'
    java_import 'java.util.concurrent.locks.ReentrantLock'

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

    class Lock
      def initialize
        @lock = ReentrantLock.new
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