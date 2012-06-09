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
end