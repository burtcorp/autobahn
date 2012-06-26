# encoding: utf-8

module Autobahn
  class Encoder
    class << self
      def encoder(content_type, options={})
        content_encoding = options[:content_encoding]
        encoder = nil
        if content_type_registry[content_type]
          encoder = content_type_registry[content_type].new
          if content_encoding
            if content_encoding_registry[content_encoding]
              encoder = content_encoding_registry[content_encoding].new(encoder)
            else
              encoder = nil
            end
          end
        end
        encoder
      end
      alias_method :[], :encoder

      private

      def content_type_registry
        @content_type_registry ||= Hash.new do |reg, ct|
          e = @encoders.find { |e| e.content_type == ct }
          reg[ct] = e if e
        end
      end

      def content_encoding_registry
        @content_encoding_registry ||= Hash.new do |reg, ce|
          e = @encoders.find { |e| e.content_encoding == ce }
          reg[ce] = e if e
        end
      end

      def register(encoder_class)
        @encoders ||= []
        @encoders << encoder_class
        @rehash = true
      end

      def inherited(c)
        register(c)
      end

      public

      begin :configuration_dsl
        def content_type(content_type=nil)
          @content_type = content_type if content_type
          @content_type
        end

        def content_encoding(content_encoding=nil)
          @content_encoding = content_encoding if content_encoding
          @content_encoding
        end

        def encodes_batches!
          @encodes_batches = true
        end

        def encodes_batches?
          !!@encodes_batches
        end
      end
    end

    def initialize(wrapped_encoder=nil)
      @wrapped_encoder = wrapped_encoder
    end

    def properties
      @properties ||= begin
        ct = self.class.content_type
        ce = self.class.content_encoding
        p = {}
        p[:content_type] = ct if ct
        p[:content_encoding] = ce if ce
        p = @wrapped_encoder.properties.merge(p) if @wrapped_encoder
        p
      end
    end

    def encodes_batches?
      if @wrapped_encoder
        @wrapped_encoder.encodes_batches?
      else
        self.class.encodes_batches?
      end
    end
  end

  class StringEncoder < Encoder
    content_type 'application/octet-stream'

    def encode(obj)
      obj.to_s
    end

    def decode(str)
      str
    end
  end

  begin
    require 'json'
  
    class JsonEncoder < Encoder
      content_type 'application/json'
      encodes_batches!

      def encode(obj)
        obj.to_json
      end

      def decode(str)
        JSON.parse(str)
      end
    end
  rescue LoadError
  end

  begin
    require 'msgpack'

    class MsgPackEncoder < Encoder
      content_type 'application/msgpack'
      encodes_batches!

      def encode(obj)
        MessagePack.pack(obj)
      end

      def decode(str)
        MessagePack.unpack(str.force_encoding(Encoding::BINARY))
      end
    end
  rescue LoadError
  end

  begin
    require 'bson'

    class BsonEncoder < Encoder
      content_type 'application/bson'
      encodes_batches!

      def encode(obj)
        BSON.serialize(obj).to_s
      end

      def decode(str)
        BSON.deserialize(str)
      end
    end
  rescue LoadError
  end

  begin
    require 'zlib'
    require 'stringio'

    class GzipEncoder < Encoder
      content_encoding 'gzip'

      def encode(obj)
        io = StringIO.new
        gz = Zlib::GzipWriter.new(io)
        gz.print(@wrapped_encoder.encode(obj))
        gz.close
        io.string
      end

      def decode(str)
        io = StringIO.new(str)
        gz = Zlib::GzipReader.new(io)
        @wrapped_encoder.decode(gz.read).tap { gz.close }
      end
    end
  end
end