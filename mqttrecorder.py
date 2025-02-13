#!/usr/bin/env python3
import sys

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import time
import yaml
import argparse


class MqttRecorder():
    """
    This picks up all communication on the topics specified in the config
    under `topics` and dumps the incoming strings (this is a strong assumption) into a file, with timestamp.
    """


    def __init__(self, config):
        self.config = config
        self.callbacks = {}
        self.out = None
        self.__init_client()

    def __init_client(self):
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        # self.client.username_pw_set(self.mqtt_username, self.mqtt_password)
        # self.client.on_connect = self.__on_mqtt_connect
        self.client.on_message = self._on_message
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_disconnect = self._on_disconnect

    def __subscribe_topics(self, topics):
        for topic in topics:
            self.register_callback(topic, self.dump_string)
        self.register_callback('/recorder', self.__recorder_msgs)

    def __recorder_msgs(self, message):
        msg = str(message.payload.decode("utf-8"))
        if msg == 'exit':
            self.mqtt_disconnect()

    def dump_string(self, message):
        now = time.time()
        msg = str(message.payload.decode("utf-8"))
        now = str(now)
        self.out.write(now + '\t' + message.topic + '\t' + msg + '\n')


    def mqtt_connect(self, forever=False):
        host = 'localhost'
        port = 1883
        if 'mqtt_address' in self.config:
            hostport = self.config['mqtt_address'].split(':')
            host = hostport[0]
            if len(hostport) > 1:
                port = int(hostport[1])
        print("connecting to: " + host + " ", end="")
        self.client.connect(host, port)
        if forever:
            self.client.loop_forever()
        else:
            self.client.loop_start()

    def mqtt_disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        print('CONNACK received with code %s. ' % str(reason_code), end="")
        self.__subscribe_topics(self.config['topics'])

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        print("Subscribed: "+str(mid)+" "+str(reason_code_list))

    def _on_message(self, client, userdata, message):
        if message.topic not in self.callbacks.keys():
            self.callbacks[message.topic] = None
            for topic in self.callbacks:
                if mqtt.topic_matches_sub(topic, message.topic):
                    self.callbacks[message.topic] = self.callbacks[topic]
        cb = self.callbacks[message.topic]
        if cb is not None:
            cb(message)
        return

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if self.out:
            self.out.close()
        print('Disconnecting...')
        self.is_running = False

    def publish(self, topic: str, message: str):
        self.client.publish(topic, message)

    def register_callback(self, topic: str, fn):
        self.client.subscribe(topic)
        self.callbacks[topic] = fn

    def record(self, output_file):
        try:
            self.__subscribe_topics(config['topics'])
            self.is_running = True
            self.out = open(output_file, 'w', encoding='utf-8')
            self.mqtt_connect(forever=False)
        except Exception as e:
            print('Exception: {}'.format(e))
            self.out.close()


    def playback(self, input_file, log_sleep=False):
        """
        playback a recorded message file. if log_sleep is True, playback tries
        to keep the same delay between messages as during recording.
        """
        start = -1
        if not self.client.is_connected():
            self.mqtt_connect(forever=False)
        lines = 0
        # wait for proper connection
        while not self.client.is_connected():
            time.sleep(0.1)
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                fields = line.strip().split('\t')
                if len(fields) < 3:
                    print('skipping line {}'.format(line))
                    continue
                when = float(fields[0])
                wait = 0
                if start < 0:
                    start = when
                else:
                    wait = when - start
                    start = when
                if log_sleep:
                    time.sleep(wait)
                lines += 1
                self.publish(fields[1], fields[2])
        time.sleep(1.0)
        print(f'{lines} messages sent')
        self.mqtt_disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='MQTT Recorder',
        description='Listen to mqtt topics and dump messages published there to a log file',
        epilog='')
    parser.add_argument("-c", "--config", type=str,
                        required=False, help='config file')
    parser.add_argument("-o", "--output-file", type=str,
                        required=False, help='message dump file')
    parser.add_argument("-p", "--playback", type=str,
                        required=False, help='playback a file')
    parser.add_argument('-k', '--keep-timing', action='store_true',
                         help='keep delay between messages')
    parser.add_argument('-n', '--no-recording', action='store_true',
                        required=False, help='no recording (for playback)')
    parser.add_argument('files', metavar='files', type=str, nargs='*')
    args = parser.parse_args()

    if args.config:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = { 'topics': ['#'] }
    if args.output_file:
        output_file = args.output_file
    elif args.files:
        output_file = args.files[0]
    elif 'output_file' in config:
        output_file = config['output_file']
    else:
        output_file = 'out.log'
    m = MqttRecorder(config)
    if not args.no_recording:
        m.record(output_file)
    else:
        config['topics'] = []
    if args.playback:
        m.playback(args.playback, log_sleep=args.keep_timing)

