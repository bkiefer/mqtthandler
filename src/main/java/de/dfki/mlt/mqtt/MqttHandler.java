package de.dfki.mlt.mqtt;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
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

  private Map<String, Predicate<byte[]>> callbacks = new HashMap<>();

  String brokerprotocol = "tcp";
  String brokerhost = "localhost";
  int brokerport = 1883;

  private class MyMqttCallback implements MqttCallback {

    /** Called when the client lost the connection to the broker */
    @Override
    public void connectionLost(Throwable cause) {
      log.error("MQTT broker connection lost: {}", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
      log.debug("Message arrived on topic {}: {} {}", topic, message.getId(), message.getQos());
      Predicate<byte[]> callback = callbacks.get(topic);
      if (callback != null) {
        // callback should return true if the payload could be converted and
        // was a valid argument to the callback
        if (callback.test(message.getPayload())) {
          try {
            client.messageArrivedComplete(message.getId(), message.getQos());
          } catch (MqttException e) {
            log.error("{}", e);
          }
        } else {
          log.warn("callback for topic {} failed.", topic);
        }
      } else {
        log.warn("No callback for registered topic {}", topic);
      }
    }

    /** Called when a outgoing publish is complete */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
      log.debug("{}", token.getMessageId());
    }
  }

  public MqttHandler(Map<String, Object> config) throws MqttException {
    int millis_reconnect = 0;

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
    connect(millis_reconnect);

    client.setCallback(new MyMqttCallback());

    // client.subscribe("#", 1); // subscribe to everything with QoS = 1

    // client.publish("topic", "payload".getBytes(UTF_8), 2, // QoS = 2
    //    false);
  }

  public void connect(int millis_reconnect) throws MqttException {
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
      options.setConnectionTimeout(millis_reconnect / 1000);
      options.setMaxReconnectDelay(millis_reconnect);
    }
    /*
    mqttConnectOptions.setUserName("<your_username>");
    mqttConnectOptions.setPassword("<your_password>".toCharArray());
    // using the default socket factory
    mqttConnectOptions.setSocketFactory(SSLSocketFactory.getDefault());
    */
    client.connect(options);
    log.info("MQTT client connected");
  }

  // TODO: do this such that wildcards work, which is currently not the case
  // because the topic in messageArrived is not properly treated.
  public void register(String topic, Predicate<byte[]> callback) throws MqttException {
    callbacks.put(topic, callback);
    client.subscribe(topic); // subscribe to everything with QoS = 1
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
      client.publish(topic, msg);
    } catch (UnsupportedEncodingException e) {
      log.error("{}", e);
    } catch (MqttPersistenceException e) {
      log.error("{}", e);
    } catch (MqttException e) {
      log.error("{}", e);
    }
  }
}
