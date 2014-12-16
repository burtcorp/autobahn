require_relative '../spec_helper'


module Autobahn
  describe QueueingConsumer do
    doubles :channel, :encoder_registry, :encoder, :demultiplexer, :headers, :encoded_message, :decoded_message

    let :queueing_consumer do
      described_class.new(channel, encoder_registry, demultiplexer)
    end
    
    describe '#deliver' do
      before do
        headers.stub(:content_type).and_return('application/stuff')
        headers.stub(:content_encoding).and_return(nil)
      end

      context 'with non-batched messages' do
        before do
          encoder.stub(:decode).with(encoded_message).and_return(decoded_message)
          encoder.stub(:encodes_batches?).and_return(false)
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

      context 'with batched messages' do
        doubles :another_decoded_message

        before do
          encoder.stub(:encodes_batches?).and_return(true)
        end

        before do
          encoder_registry.stub(:[]).with('application/stuff', anything).and_return(encoder)
        end

        it 'decodes using an encoder retrieved from the encoder registry, and passes each message in the batch to the demultiplexer' do
          encoder.stub(:decode).with(encoded_message).and_return([decoded_message, another_decoded_message])
          demultiplexer.should_receive(:put).with([an_instance_of(BatchHeaders), decoded_message])
          demultiplexer.should_receive(:put).with([an_instance_of(BatchHeaders), another_decoded_message])
          queueing_consumer.deliver(headers, encoded_message)
        end

        it 'decodes using an encoder retrieved from the encoder registry, and passes each message in the batch to the demultiplexer, when the batch contains only one message' do
          encoder.stub(:decode).with(encoded_message).and_return([decoded_message])
          demultiplexer.should_receive(:put).with([headers, decoded_message])
          queueing_consumer.deliver(headers, encoded_message)
        end

        it 'acks empty batches immediately' do
          encoder.stub(:decode).and_return([])
          demultiplexer.should_not_receive(:put)
          headers.should_receive(:ack)
          queueing_consumer.deliver(headers, encoded_message)
        end
      end
    end
  end
end