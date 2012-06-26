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

  describe GzipEncoder do
    let :wrapped_encoder do
      JsonEncoder.new
    end

    let :gzip_encoder do
      GzipEncoder.new(wrapped_encoder)
    end

    describe '#properties' do
      it 'inherits its content type from the encoder it wraps' do
        gzip_encoder.properties[:content_type].should == wrapped_encoder.properties[:content_type]
      end

      it 'specifies the content encoding' do
        gzip_encoder.properties[:content_encoding].should == 'gzip'
      end
    end
  end
end