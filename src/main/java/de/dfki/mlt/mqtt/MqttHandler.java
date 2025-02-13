package de.dfki.mlt.mqtt;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.function.Predicate;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttHandler {
  private static final Logger log = LoggerFactory.getLogger(MqttHandler.class);

  public static final String CFG_MQTT_HOST = "brokerhost";
  public static final String CFG_MQTT_PORT = "brokerport";
  public static final String CFG_MQTT_PROTOCOL = "brokerprotocol";
  public static final String CFG_MQTT_MILLIS_RECONNECT = "millis_reconnect";

  private int msgId = 0;
  private int defaultQos = 1;

  private String myClientId = null;
  private MqttClient client;

  String brokerprotocol = "tcp";
  String brokerhost = "localhost";
  int brokerport = 1883;
  int millis_reconnect = 0;

  public MqttHandler(Map<String, Object> config) throws MqttException {
    if (config != null) {
      if (config.containsKey(CFG_MQTT_HOST)) {
        brokerhost = (String) config.get(CFG_MQTT_HOST);
      }
      if (config.containsKey(CFG_MQTT_PORT)) {
        brokerport = (Integer) config.get(CFG_MQTT_PORT);
      }
      if (config.containsKey(CFG_MQTT_PROTOCOL)) {
        brokerprotocol = (String) config.get(CFG_MQTT_PROTOCOL);
      }
      if (config.containsKey(CFG_MQTT_MILLIS_RECONNECT)) {
        millis_reconnect = (Integer) config.get(CFG_MQTT_MILLIS_RECONNECT);
      }
    }
    // if MILLIS_RECONNECT is zero, there will be no attempt to reconnect
    connect();

    //client.setCallback(new MyMqttCallback());

    // client.subscribe("#", 1); // subscribe to everything with QoS = 1

    // client.publish("topic", "payload".getBytes(UTF_8), 2, // QoS = 2
    //    false);
  }

  public void connect() throws MqttException {
    if (myClientId == null) {
      myClientId = MqttClient.generateClientId();
    }
    // serverURI in format: "protocol://name:port"
    client =
        new MqttClient(
            brokerprotocol + "://" + brokerhost + ":" + brokerport,
            myClientId, // ClientId
            new MemoryPersistence()); // Persistence

    MqttConnectOptions options = new MqttConnectOptions();
    if (millis_reconnect > 0) {
      options.setAutomaticReconnect(true);
      //options.setConnectionTimeout(millis_reconnect / 1000);
      //options.setMaxReconnectDelay(millis_reconnect);
    }
    /*
    mqttConnectOptions.setUserName("<your_username>");
    mqttConnectOptions.setPassword("<your_password>".toCharArray());
    // using the default socket factory
    mqttConnectOptions.setSocketFactory(SSLSocketFactory.getDefault());
    */
    try {
      client.connect(options);
    } catch (Exception ex) {
      log.error("Connecting MQTT client on {}:{} failed: {}",
          brokerhost, brokerport, ex.getMessage());
      return;
    }
    log.info("MQTT client connected");
  }

  /** Subscribe a callback that takes a message payload and processes it
   *  immediately, returning true if the payload is as the callback expects it
   *  to be.
   *
   * @param topic the topic to subscribe to. Can contain wildcards, handled by
   *        the underlying library
   * @param callback a predicate returning true if the payload could be
   *        processed properly
   * @throws MqttException if the subscription fails for some reason
   */
  public void register(String topic, Predicate<byte[]> callback)
      throws MqttException {
    // subscribe to everything with QoS = 1
    client.subscribe(topic, new IMqttMessageListener() {

      @Override
      public void messageArrived(String topic, MqttMessage message)
          throws Exception {
        if (callback.test(message.getPayload())) {
          try {
            client.messageArrivedComplete(message.getId(), message.getQos());
          } catch (MqttException e) {
            log.error("{}", e);
          }
        } else {
          log.warn("callback for topic {} failed.", topic);
        }
      }});
  }

  public void disconnect() throws MqttException {
    client.disconnect();
  }

  public void sendMessage(String topic, String payload) {
    sendMessage(topic, payload, msgId++, defaultQos);
  }

  public void sendMessage(String topic, String payload, int id, int qos) {
    MqttMessage msg;
    try {
      msg = new MqttMessage(payload.getBytes("UTF-8"));
      msg.setId(id);
      msg.setQos(qos);
      while (!client.isConnected()) {
        log.info("Waiting for reconnect");
        client.connect();
        Thread.sleep(millis_reconnect);
      }
      client.publish(topic, msg);
    } catch (UnsupportedEncodingException e) {
      log.error("{}", e);
    } catch (MqttPersistenceException e) {
      log.error("{}", e);
    } catch (MqttException e) {
      log.error("{}", e);
    } catch (InterruptedException e) {
    }
  }
}
