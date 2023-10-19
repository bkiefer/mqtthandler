#!/bin/sh
scrdir=`dirname $0`
cd "$scrdir"
docker container ls | grep -q mqtt-broker ||
    docker run --name mqtt-broker -d --rm -p 1883:1883 -p 9001:9001 \
           -v `pwd`/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro \
           eclipse-mosquitto:2.0.15
