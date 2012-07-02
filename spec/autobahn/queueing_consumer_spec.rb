require_relative '../spec_helper'


module Autobahn
  describe QueueingConsumer do
    stubs :channel, :encoder_registry, :encoder, :demultiplexer, :headers, :encoded_message, :decoded_message

    let :queueing_consumer do
      described_class.new(channel, encoder_registry, demultiplexer)
    end

    before do
      encoder.stub(:decode).with(encoded_message).and_return(decoded_message)
    end
    
    describe '#deliver' do
      before do
        headers.stub(:content_type).and_return('application/stuff')
        headers.stub(:content_encoding).and_return(nil)
      end

      it 'decodes using an encoder retrieved from the encoder registry' do
        encoder_registry.stub(:[]).with('application/stuff', anything).and_return(encoder)
        demultiplexer.should_receive(:put).with([headers, decoded_message])
        queueing_consumer.deliver(headers, encoded_message)
      end

      it 'decodes using an encoder retrieved from the encoder registry, when theres a content encoding' do
        headers.stub(:content_encoding).and_return('fractal')
        encoder_registry.stub(:[]).with('application/stuff', :content_encoding => 'fractal').and_return(encoder)
        demultiplexer.should_receive(:put).with([headers, decoded_message])
        queueing_consumer.deliver(headers, encoded_message)
      end
    end
  end
end