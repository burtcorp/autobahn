# encoding: utf-8

require 'socket'
require 'spec_helper'

module Autobahn
  describe Cluster do
    let :cluster do
      Cluster.new("http://localhost:55672/api")
    end

    before :all do
      WebMock.enable!
    end

    after :all do
      WebMock.disable!
    end

    context '#overview' do
      it 'handles legacy redirect' do
        stub_request(:get, "http://guest:guest@localhost:55672/api/overview").to_return(
          :status => [301, "Moved Permanently"],
          :headers => { "Location" => "http://localhost:15672/api/overview" })
        stub_request(:get, "http://localhost:15672/api/overview").to_return(
          :status => [401, "Not Authorized"])
        stub_request(:get, "http://guest:guest@localhost:15672/api/overview").to_return(
          :body => "{}")

        cluster.overview.should == {}
      end

      context 'with nil API URI' do
        let :cluster do
          Cluster.new(nil)
        end

        let :stub_legacy_request do
          stub_request(:get, "http://guest:guest@localhost:55672/api/overview")
        end

        let :stub_modern_request do
          stub_request(:get, "http://guest:guest@localhost:15672/api/overview")
        end

        it 'connects to localhost:15672 for testing purposes' do
          stub_modern_request.to_return(:body => '{}')

          cluster.overview.should == {}
        end

        it 'reverts to legacy localhost:55672 when the connection fails' do
          stub_modern_request.to_raise(Errno::ECONNREFUSED)
          stub_legacy_request.to_return(:body => '{}')

          cluster.overview.should == {}
        end

        it 'caches the URI after the first request' do
          stub_modern_request.to_raise(Errno::ECONNREFUSED)
          stub_modern_request.to_return(:body => '{}')

          stub_legacy_request.to_return(:body => '{}')
          stub_legacy_request.to_raise(Errno::ECONNREFUSED)

          cluster.overview.should == {}
          expect { cluster.overview }.to raise_error(Errno::ECONNREFUSED)
        end
      end
    end
  end
end
