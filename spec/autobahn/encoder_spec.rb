require_relative '../spec_helper'



module Autobahn
  class BogusEncoder < Encoder
    content_type 'application/x-stuff'
    content_encoding 'lolcode'
    encodes_batches!
  end

  describe Encoder do
    describe '.[]' do
      it 'creates an encoder that encodes and decodes the specified content type' do
        Encoder['application/x-stuff'].should be_a(BogusEncoder)
        Encoder['application/json'].should be_a(JsonEncoder)
      end

      it 'wraps an encoder in another encoder that encodes and decodes the specified content encoding' do
        Encoder['application/json', :content_encoding => 'gzip'].should be_a(GzipEncoder)
      end

      it 'returns nil if no encoder for the content encoding could be found, even if an encoder for the content type exists' do
        Encoder['application/json', :content_encoding => 'magic'].should be_nil
      end

      it 'caches encoders' do
        instance1 = Encoder['application/json', :content_encoding => 'gzip']
        instance2 = Encoder['application/json', :content_encoding => 'gzip']
        instance1.should equal(instance2)
      end

      it 'prefers encoders that support both the content type and encoding' do
        combined_encoder = Encoder['application/msgpack', content_encoding: 'lzf']
        combined_encoder.should be_a(MsgPackLzfEncoder)
      end
    end
  end

  describe 'An encoder' do
    let :encoder do
      BogusEncoder.new
    end

    describe '#properties' do
      it 'has contains the content type' do
        encoder.properties[:content_type].should == 'application/x-stuff'
      end

      it 'contains the content encoding' do
        encoder.properties[:content_encoding].should == 'lolcode'
      end
    end

    describe '#encodes_batches?' do
      it 'is true when encodes_batches! was specified' do
        encoder.encodes_batches?.should be_true
      end
    end
  end

  shared_examples 'encoders' do |encoding_name, content_type=nil|
    it 'compresses and decompresses data' do
      encoder.decode(encoder.encode({'hello' => 'world'})).should == {'hello' => 'world'}
    end

    describe '#encode' do
      it 'returns a binary string' do
        encoder.encode({'hello' => 'world'}).encoding.should == Encoding::BINARY
      end
    end

    describe '#properties' do
      it 'inherits its content type from the encoder it wraps' do
        expected_content_type = (defined? wrapped_encoder) ? wrapped_encoder.properties[:content_type] : content_type
        encoder.properties[:content_type].should == expected_content_type
      end

      it 'specifies the content encoding' do
        encoder.properties[:content_encoding].should == encoding_name
      end
    end
  end

  {GzipEncoder => 'gzip', LzfEncoder => 'lzf', Lz4Encoder => 'lz4'}.each do |encoder_class, encoding_name|
    describe encoder_class do
      let :wrapped_encoder do
        JsonEncoder.new
      end

      let :encoder do
        encoder_class.new(wrapped_encoder)
      end

      include_examples 'encoders', encoding_name
    end
  end

  {
    'lzf' => [MsgPackLzfEncoder, LzfEncoder],
    'lz4' => [MsgPackLz4Encoder, Lz4Encoder],
  }.each do |encoding_name, (combined_encoder_class, compressing_encoder_class)|
    describe combined_encoder_class do
      let :encoder do
        combined_encoder_class.new
      end

      include_examples 'encoders', encoding_name, 'application/msgpack'

      it 'is compatible with using the two separate encoders' do
        combined_encoder = encoder
        uncombined_encoder = compressing_encoder_class.new(Encoder['application/msgpack'])
        message = {'hello' => 'world'}
        uncombined_encoder.decode(combined_encoder.encode(message)).should == message
        combined_encoder.decode(uncombined_encoder.encode(message)).should == message
      end

      describe '.content_encoding' do
        it 'returns its content encoding' do
          combined_encoder_class.content_encoding.should == encoding_name
        end
      end

      describe '.content_type' do
        it 'returns its content type' do
          combined_encoder_class.content_type.should == 'application/msgpack'
        end
      end

      context 'when specifying options' do
        it 'can decode hash keys as symbols' do
          encoder = combined_encoder_class.new(symbolize_keys: true)
          decoded = encoder.decode(encoder.encode('foo' => 'bar', 'baz' => [{'qux' => 'zuul'}]))
          decoded.should eq(:foo => 'bar', :baz => [{:qux => 'zuul'}])
        end
      end
    end
  end
end