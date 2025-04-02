#!/usr/bin/env -S python3 -u
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import logging
import time
import yaml
import argparse

# configure logger
logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(message)s",
    level=logging.INFO)
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

class MqttRecorder():
    """
    This picks up all communication on the topics specified in the config
    under `topics` and dumps the incoming strings (this is a strong assumption) into a file, with timestamp.
    """

    def __init__(self, config):
        self.config = config
        self.topics = {}
        for topic in self.config['topics']:
            if type(topic) is dict:
                top = topic['topic']
                # is callback function specified?
                if 'callback' in topic:
                    self.topics[top] = topic['callback']
                else:
                    self.topics[top] = self.dump_string
                if 'qos' in topic:
                    self.topics[top] = (self.topics[top], topic['qos'])
            else:
                self.topics[topic] = self.dump_string
        self.topics['/recorder'] = self.__recorder_msgs
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

    def __recorder_msgs(self, client, userdata, message):
        msg = str(message.payload.decode("utf-8"))
        if msg == 'exit':
            self.is_running = False
            self.mqtt_disconnect()

    def dump_string(self, client, userdata, message):
        now = time.time()
        msg = str(message.payload.decode("utf-8"))
        now = str(now)
        self.out.write(now + '\t' + message.topic + '\t' + msg + '\n')


    def mqtt_connect(self, wait_forever=False):
        host = 'localhost'
        port = 1883
        if 'mqtt_address' in self.config:
            hostport = self.config['mqtt_address'].split(':')
            host = hostport[0]
            if len(hostport) > 1:
                port = int(hostport[1])
        logger.info(f"connecting to: {host}:{port}")
        self.client.connect(host, port)
        if wait_forever:
            self.client.loop_forever()
        else:
            self.client.loop_start()

    def mqtt_disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f'CONNACK received with code {str(reason_code)}')
        # subscribe to all registered topics/callbacks
        for topic in self.topics:
            qos = 0
            if topic is tuple:
                qos = topic[1]
                topic = topic[0]
            self.client.subscribe(topic, qos)

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        logger.debug("Subscribed: "+str(properties)+" "+str(reason_code_list))

    def _on_message(self, client, userdata, message):
        logger.debug(f"Received message {str(message.payload)} on topic {message.topic} with QoS {str(message.qos)}")
        if message.topic not in self.topics:
            self.topics[message.topic] = None
            for topic in self.topics:
                if mqtt.topic_matches_sub(topic, message.topic):
                    self.topics[message.topic] = self.topics[topic]
        cb = self.topics[message.topic]
        if cb is not None:
            if cb is tuple:
                cb = cb[0]  # second is qos
            cb(client, userdata, message)
        return

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if self.out:
            self.out.close()
        logger.info('Disconnecting...')
        self.is_running = False

    def publish(self, topic: str, message: str):
        self.client.publish(topic, message)

    def record(self, output_file, wait_forever=True):
        try:
            self.is_running = True
            self.out = open(output_file, 'w', encoding='utf-8')
            self.mqtt_connect(wait_forever=wait_forever)
        except Exception as e:
            logger.error('Exception: {}'.format(e))
            self.out.close()


    def playback(self, input_file, log_sleep=False):
        """
        playback a recorded message file. if log_sleep is True, playback tries
        to keep the same delay between messages as during recording.
        """
        start = -1
        if not self.client.is_connected():
            self.mqtt_connect()
        lines = 0
        # wait for proper connection
        while not self.client.is_connected():
            time.sleep(0.1)
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                fields = line.strip().split('\t')
                if len(fields) < 3:
                    logger.info('skipping line {}'.format(line))
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
        logger.info(f'{lines} messages sent')
        self.mqtt_disconnect()

def main():
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
    parser.add_argument('-d', '--delay', action='store_true',
                         help='keep delay between messages')
    parser.add_argument('-n', '--no-recording', action='store_true',
                        required=False, help='no recording (for playback)')
    parser.add_argument('files', metavar='files', type=str, nargs='*')
    args = parser.parse_args()

    if args.config:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = { 'topics': { '#' } }
    if args.output_file:
        output_file = args.output_file
    elif 'output_file' in config:
        output_file = config['output_file']
    else:
        output_file = 'mqtt.log'
    if args.no_recording:
        config['topics'] = []
    m = MqttRecorder(config)
    if not args.no_recording:
        m.record(output_file, not args.playback)
    if args.playback:
        m.playback(args.playback, log_sleep=args.delay)

if __name__ == '__main__':
    main()
