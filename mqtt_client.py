import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import argparse
import logging

logger: logging.Logger
logger = logging.getLogger(__file__)

class MqttClient():
    valid_mqtt_keys = { 'host', 'port', 'bind_address', 'bind_port',
                        'keepalive', 'clean_start',
                        'username', 'password' }

    def __init__(self, pid: str, config: dict):
        self.pid = pid
        self.topics = {}
        for topic_spec in config.get('topics',[]):
            topic = None
            cb = ""
            qos = None
            if isinstance(topic_spec, dict):
                topic = topic_spec['topic']
                # is callback function specified?
                if 'callback' in topic_spec:
                    cb = topic_spec['callback']
                else:
                    logger.error(f"Missing callback for {topic}, ignoring")
                    continue
                if 'qos' in topic_spec:
                    qos = topic_spec['qos']
            elif isinstance(topic_spec, tuple):
                topic = topic_spec[0]
                cb = topic_spec[1]
                if len(topic_spec) > 2:
                    qos = topic_spec[2]
            if isinstance(cb, str):
                cb = eval(cb)
            if qos is not None:
                cb = (cb, qos)
            self.topics[topic] = cb

        self.mqtt_config = { 'host': 'localhost' }
        self.mqtt_config.update(config)

        for key in filter(lambda k: k not in self.__class__.valid_mqtt_keys, config.keys()):
                del self.mqtt_config[key]
        self.client: mqtt.Client
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        if 'username' in self.mqtt_config and 'password' in self.mqtt_config:
            self.client.username_pw_set(self.mqtt_config['username'],
                                        self.mqtt_config['password'])
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        logger.debug(f'CONNACK received with code {reason_code}')
        # subscribe to all registered topics/callbacks
        for topic in self.topics:
            cb = self.topics[topic]
            if isinstance(cb, tuple):
                self.client.subscribe(topic, cb[1])
            else:
                self.client.subscribe(topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        logger.info('Disconnecting...')
        self.is_running = False

    def _on_subscribe(self, client, userdata, mid, reason_code_list, props):
        logger.debug("Subscribed: "+str(props)+" "+str(reason_code_list))

    def _on_message(self, client, userdata, message):
        logger.debug(f"Received message {str(message.payload)} on topic {message.topic} with QoS {str(message.qos)}")
        if message.topic not in self.topics:
            self.topics[message.topic] = None
            for topic in self.topics:
                if mqtt.topic_matches_sub(topic, message.topic):
                    self.topics[message.topic] = self.topics[topic]
        cb = self.topics[message.topic]
        if cb is not None:
            if isinstance(cb, tuple):
                cb = cb[0]  # second is qos
            cb(client, userdata, message)
        return

    def mqtt_connect(self, forever = False):
        logger.debug("Connecting to broker on {}", self.mqtt_config['host']);
        self.client.connect(**self.mqtt_config)
        if forever:
            self.client.loop_forever()
        else:
            self.client.loop_start()

    def mqtt_disconnect(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()

    def publish(self, topic: str, message: str):
        self.client.publish(topic, message)


def print_string(client, userdata, message):
    print(f'\n{message.payload.decode("utf-8")}\n')

def main():
    parser = argparse.ArgumentParser(
        prog='MQTT Client',
        description='published messages to a given topic',
        epilog='')
    parser.add_argument("-H", "--host", type=str,
                        required=False, help='broker host')
    parser.add_argument("-t", "--topic", type=str,
                        required=True, help='topic to publish messages to')
    parser.add_argument("-p", "--port", type=int,
                        required=False, help='broker port')
    parser.add_argument("-i", "--interactive", action='store_true',
                        default=False, required=False,
                        help='ask for messages interactively')
    parser.add_argument("-l", "--listen", action='store_true', default=False,
                        required=False, help='print incoming messages')
    parser.add_argument('msgs', metavar='msgs', type=str, nargs='*')
    args = parser.parse_args()

    config = {}
    if args.listen:
        config['topics'] = [( '#', "print_string" )]
    if args.host:
        config['host'] = args.host
    if args.port:
        config['port'] = args.port
    m = MqttClient('cliclient', config)
    m.mqtt_connect(False)
    for f in args.msgs:
        m.publish(args.topic, f)
    if args.interactive:
        while True:
            inp = input(">")
            if not inp:
                break
            m.publish(args.topic, inp)
    m.mqtt_disconnect()


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s: %(levelname)s: %(message)s",
                        level=logging.WARN)
    #mqtt_client.logger.setLevel(logging.DEBUG)
    main()
