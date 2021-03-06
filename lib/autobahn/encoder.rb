# encoding: utf-8

module Autobahn
  class Encoder
    class << self
      def encoder(content_type, options={})
        content_encoding = options[:content_encoding]
        encoder = nil
        if content_type && (encoder_types = content_type_registry[content_type])
          encoder = cached_instance(content_type, content_encoding) do
            if content_encoding
              combined_type = encoder_types.find { |e| e.content_encoding == content_encoding }
              if combined_type
                combined_type.new
              else
                wrapper_type = content_encoding_registry[content_encoding]
                if wrapper_type
                  wrapper_type.new(encoder_types.first.new)
                else
                  nil
                end
              end
            else
              encoder_types.first.new
            end
          end
        end
        encoder
      end
      alias_method :[], :encoder

      private

      def cached_instance(content_type, content_encoding)
        @cached_instances ||= Hash.new { |h, k| h[k] = {} }
        @cached_instances[content_type][content_encoding] ||= yield
      end

      def content_type_registry
        @content_type_registry ||= Hash.new do |reg, ct|
          e = @encoders.select { |e| e.content_type == ct }
          reg[ct] = e unless e.empty?
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

  class GzipEncoder < Encoder
    java_import 'java.util.zip.GZIPOutputStream'
    java_import 'java.util.zip.GZIPInputStream'
    java_import 'java.io.ByteArrayOutputStream'
    java_import 'java.io.ByteArrayInputStream'

    content_encoding 'gzip'

    def encode(obj)
      baos = ByteArrayOutputStream.new
      gzos = GZIPOutputStream.new(baos)
      strb = @wrapped_encoder.encode(obj).to_java_bytes
      gzos.write(strb, 0, strb.length)
      gzos.close
      String.from_java_bytes(baos.to_byte_array)
    end

    def decode(str)
      bais = ByteArrayInputStream.new(str.to_java_bytes)
      gzis = GZIPInputStream.new(bais)
      output = ''.force_encoding(Encoding::BINARY)
      buffer = Java::byte[1024 * 16].new
      until gzis.available == 0
        bytes_read = gzis.read(buffer)
        if bytes_read > 0
          output << String.from_java_bytes(buffer)[0, bytes_read]
        end
      end
      gzis.close
      @wrapped_encoder.decode(output)
    end
  end

  begin
    require 'ning-compress-jars'

    class LzfEncoder < Encoder
      java_import 'com.ning.compress.lzf.LZFEncoder'
      java_import 'com.ning.compress.lzf.LZFDecoder'

      content_encoding 'lzf'

      def encode(obj)
        String.from_java_bytes(LZFEncoder.encode(@wrapped_encoder.encode(obj).to_java_bytes))
      end

      def decode(str)
        @wrapped_encoder.decode(String.from_java_bytes(LZFDecoder.decode(str.to_java_bytes)))
      end
    end
  rescue LoadError
  end

  begin
    require 'lz4-ruby'
    class Lz4Encoder < Encoder
      content_encoding 'lz4'

      def encode(obj)
        LZ4.compress(@wrapped_encoder.encode(obj))
      end

      def decode(str)
        @wrapped_encoder.decode(LZ4.uncompress(str))
      end
    end
  rescue LoadError
  end

  if (defined? MsgPackEncoder) && ((defined? LzfEncoder) || (defined? Lz4Encoder))
    begin
      require 'autobahn_msgpack'

      begin
        Java::ComHeadiusJrubyLz4VendorNetJpountzLz4::LZ4Factory
        Java::AutobahnEncoder::MsgPackLz4Encoder.load(JRuby.runtime)
      rescue NameError
      end

      begin
        Java::ComNingCompressLzf::LZFEncoder
        Java::AutobahnEncoder::MsgPackLzfEncoder.load(JRuby.runtime)
      rescue NameError
      end
    rescue LoadError
      $stderr.puts "warning: Failed to load autobahn_msgpack.jar. Run `rake build` to create it."
    end
  end
end
