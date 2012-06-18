# encoding: utf-8

module Autobahn
  class StringEncoder
    PROPERTIES = {:content_type => 'application/octet-stream'.freeze}.freeze

    def properties
      PROPERTIES
    end

    def encodes_batches?
      false
    end

    def encode(obj)
      obj.to_s
    end

    def decode(str)
      str
    end
  end

  begin
    require 'json'
  
    class JsonEncoder
      PROPERTIES = {:content_type => 'application/json'.freeze}.freeze

      def properties
        PROPERTIES
      end

      def encodes_batches?
        true
      end

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

    class MsgPackEncoder
      PROPERTIES = {:content_type => 'application/msgpack'.freeze}.freeze

      def properties
        PROPERTIES
      end

      def encodes_batches?
        true
      end

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

    class BsonEncoder
      PROPERTIES = {:content_type => 'application/bson'.freeze}.freeze

      def content_type
        CONTENT_TYPE
      end
      
      def encodes_batches?
        true
      end

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

    class GzipEncoder
      CONTENT_ENCODING = 'gzip'.freeze

      def initialize(decorated_encoder)
        @decorated_encoder = decorated_encoder
      end

      def properties
        @properties ||= begin
          p = @decorated_encoder.properties.dup
          p[:content_encoding] = CONTENT_ENCODING
          p
        end
      end

      def encodes_batches?
        @decorated_encoder.encodes_batches?
      end

      def encode(obj)
        io = StringIO.new
        gz = Zlib::GzipWriter.new(io)
        gz.print(@decorated_encoder.encode(obj))
        gz.close
        io.string
      end

      def decode(str)
        io = StringIO.new(str)
        gz = Zlib::GzipReader.new(io)
        @decorated_encoder.decode(gz.read).tap { gz.close }
      end
    end
  end
end