# mqtthandler

[![mvn build](https://github.com/bkiefer/mqtthandler/actions/workflows/maven.yml/badge.svg)](https://github.com/bkiefer/mqtthandler/actions/workflows/maven.yml)

A simple wrapper around the paho MQTT library to facilitate messaging via JSON objects serialized into strings.

Subscribe now also supports wildcards in topics, using the underlying functionality.

When calling `mvn install` or `mvn test`, make sure a MQTT broker on localhost is running and accepting anonymous connections, otherwise the tests will be skipped.
