# Autobahn

<img src="http://upload.wikimedia.org/wikipedia/commons/thumb/b/b4/Zeichen_330.svg/200px-Zeichen_330.svg.png" alt="Autobahn" style="float: left; margin-right: 1em; margin-bottom: 1em; width: 80px; height: 80px;"> Autobahn is a transport system abstraction for RabbitMQ. To get maximum performance for raw data transport you need to use multiple queues, more than one node, connect to the node that hosts the queue, batch your messages, encode your messages using an efficient serialization mechanism, and many more things depending on your exact use case. Autobahn abstracts and automates most of these things, letting you focus on the rest of your application.

## Getting started

The following example configures transport system that will transport messages JSON encoded over an existing transport system. It will look for the named exchange and discover queues and bindings using the RabbitMQ API. You only need to tell it about one node that runs the management plugin, which provides the REST API, and it will discover the topology of the cluster.

    # use this setup code on both the publisher and consumer sides
    rmq_api_uri = 'http://rmqhost:55672/api'
    exchange_name = 'stuff'
    options = {:encoder => Autobahn::JsonEncoder.new}
    transport_system = Autobahn.transport_system(rmq_api_uri, exchange_name, options)

    # do this on the publisher side
    publisher = transport_system.publisher
    loop do
      # create_next_message is whatever code you need to create the next message to publish
      publisher.publish(create_next_message)
    end

    # do this on the consumer side
    consumer = transport_system.consumer
    # the block passed to subscribe will be called asynchronously when new messages arrive
    consumer.subscribe do |headers, message|
      # process_message is whatever code you need to process a new message
      process_message(message)
      headers.ack
    end

## Requirements

* JRuby 1.7.x (might still work with 1.6.x)
* March Hare 2.7 or newer
