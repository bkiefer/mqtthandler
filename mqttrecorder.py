#!/usr/bin/env -S python3 -u
from mqtt_client import MqttClient
import logging
import time
import yaml
import argparse

# configure logger
logger = logging.getLogger(__file__)

class MqttRecorder(MqttClient):
    """
    This picks up all communication on the topics specified in the config
    under `topics` and dumps the incoming strings (this is a strong assumption) into a file, with timestamp.
    """

    def __control_msgs(self, client, userdata, message):
        """Process simple control messages, such as 'exit'."""
        msg = str(message.payload.decode("utf-8")).strip()
        if msg == 'exit':
            self.is_running = False
            self.mqtt_disconnect()

    def dump_string(self, client, userdata, message):
        if self.out:
            now = time.time()
            msg = str(message.payload.decode("utf-8"))
            now = str(now)
            self.out.write(now + '\t' + message.topic + '\t' + msg + '\n')
            self.out.flush()

    def __init__(self, config):
        if "topics" not in config:
            config["topics"] = []
        config["topics"].append(('recorder/control', self.__control_msgs))
        self.out = None
        super().__init__("recorder", config)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if self.out:
            self.out.close()
        super()._on_disconnect(client, userdata, flags, reason_code, properties)

    def record(self, output_file, wait_forever=True):
        try:
            self.is_running = True
            self.out = open(output_file, 'w', encoding='utf-8')
            self.mqtt_connect(forever=wait_forever)
        except Exception as e:
            logger.error('Exception: {}'.format(e))
            self.mqtt_disconnect()


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
    parser.add_argument("-P", "--port", type=int,
                        required=False, help='the port of the broker')
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
        config = { 'topics': [( '#', "self.dump_string" )] }
    if args.output_file:
        output_file = args.output_file
    elif 'output_file' in config:
        output_file = config['output_file']
    else:
        output_file = 'mqtt.log'
    if args.no_recording:
        config['topics'] = []
    if args.port:
        config['port'] = args.port
    m = MqttRecorder(config)
    if not args.no_recording:
        m.record(output_file, not args.playback)
    if args.playback:
        m.playback(args.playback, log_sleep=args.delay)

if __name__ == '__main__':
    logging.basicConfig(
        format="%(asctime)s: %(levelname)s: %(message)s",
        level=logging.INFO)
    #mqtt_client.logger.setLevel(logging.DEBUG)
    main()
