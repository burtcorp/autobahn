# encoding: utf-8

require 'httpclient'
require 'json'


module Autobahn
  module RestClient
    module InstanceMethods
      def configure_rest_client(base_url, user, password)
        @base_url = base_url
        @http_client = HTTPClient.new
        @http_client.set_auth(@base_url, user, password)
      end

      def api_call(entity, id=nil)
        url = "#@base_url/#{entity}"
        url << "/#{id}" if id
        JSON.parse(@http_client.get_content(url))
      end
    end

    module ClassMethods
      def entity(name, options={})
        if options[:singleton]
          define_method(name) { api_call(name) }
        else
          plural = "#{name}s".to_sym
          define_method(plural) { api_call(plural) }
          define_method(name) { |id| api_call(plural, id) }
        end
      end
    end

    def self.included(m)
      m.send(:include, InstanceMethods)
      m.send(:extend, ClassMethods)
    end
  end

  class Cluster
    include RestClient

    entity :nodes
    entity :channel
    entity :exchange
    entity :queue
    entity :binding
    entity :overview, :singleton => true

    def initialize(api_uri, options={})
      user = options[:user] || 'guest'
      password = options[:password] || 'guest'
      configure_rest_client(api_uri, user, password)
    end
  end
end