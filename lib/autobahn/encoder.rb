# encoding: utf-8

module Autobahn
  class StringEncoder
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