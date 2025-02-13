package de.dfki.mlt.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.UnsupportedEncodingException;
import java.util.function.Predicate;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTransfer {
  static Logger logger = LoggerFactory.getLogger(TestTransfer.class);

  String result = null;
  String result2 = null;

  @Test
  public void test()  {

    // start a listener, make sure a broker is running somewhere

    try {
      MqttHandler listener = new MqttHandler(null);
      listener.register("/my/test/topic", new Predicate<byte[]>() {
        @Override
        public boolean test(byte[] t) {
          try {
            // turn t into string
            result = new String(t, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            return false;
          }
          return true;
        }

      });

      listener.register("/my/+/topic", new Predicate<byte[]>() {
        @Override
        public boolean test(byte[] t) {
          try {
            // turn t into string
            result2 = new String(t, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            return false;
          }
          return true;
        }

      });
    } catch (MqttException mex) {
      if (mex.getReasonCode() == MqttException.REASON_CODE_CLIENT_NOT_CONNECTED){
        logger.warn("No MQTT connection possible, skipping test");
        assumeTrue(false);
      }
    }

    // start another client to send a message
    try {
      MqttHandler sender = new MqttHandler(null);
      sender.sendMessage("/my/test/topic", "msg1");
      Thread.sleep(500);
      sender.sendMessage("/my/test2/topic", "msg2");
      sender.sendMessage("/my/test/topic2", "msg3");
      Thread.sleep(1000);
      // make sure the messages were picked up
      assertEquals("msg1", result);
      assertEquals("msg2", result2);
    } catch (MqttException mex) {
      assertTrue(false);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
