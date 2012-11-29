# RESTful proxy for Kafka

A simple interface to Kafka that captures the complex consumer logic without the hassle.

Produce messages like:

    POST /topic
    some data

Consume messages like:

    GET /topic/group

## Compile

    sbt compile

## Run

    sbt "run config/rest.properties config/consumer.properties config/producer.properties"
