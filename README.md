# mqtthandler

[![mvn build](https://github.com/bkiefer/mqtthandler/actions/workflows/maven.yml/badge.svg)](https://github.com/bkiefer/mqtthandler/actions/workflows/maven.yml)

A simple wrapper around the paho MQTT library to facilitate messaging via JSON objects serialized into strings.

Subscribe now also supports wildcards in topics, using the underlying functionality.

When calling `mvn install` or `mvn test`, make sure a MQTT broker on localhost is running and accepting anonymous connections, otherwise the tests will be skipped.

Also included is a python script (`mqttrecorder.py`) to record arbitrary topics and write the messages to a log file as well as replaying such log files. The payloads have to be strings for the script to work. A `requirements.txt` containing the required python packages is also included.
