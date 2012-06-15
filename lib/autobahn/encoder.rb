# encoding: utf-8

module Autobahn
  class StringEncoder
    CONTENT_TYPE = 'application/octet-stream'.freeze

    def content_type
      CONTENT_TYPE
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
      CONTENT_TYPE = 'application/json'.freeze

    def content_type
      CONTENT_TYPE
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
      CONTENT_TYPE = 'application/msgpack'.freeze

      def content_type
        CONTENT_TYPE
      end

      def encodes_batches?
        true
      end

      def encode(obj)
        MessagePack.pack(obj)
      end

      def decode(str)
        MessagePack.unpack(str.force_encoding(Encoding::ASCII_8BIT))
      end
    end
  rescue LoadError
  end

  begin
    require 'bson'

    class BsonEncoder
      CONTENT_TYPE = 'application/bson'.freeze

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
end