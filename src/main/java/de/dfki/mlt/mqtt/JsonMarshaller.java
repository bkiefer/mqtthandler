package de.dfki.mlt.mqtt;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMarshaller {
  private final static Logger logger = LoggerFactory.getLogger(JsonMarshaller.class);

  private final ObjectMapper mapper;

  public JsonMarshaller() {
    mapper = new ObjectMapper();
  }

  public <R> Optional<R> unmarshal(byte[] payload, Class<R> clazz) {
    R result;
    try {
      result = mapper.readValue(
          new InputStreamReader(new ByteArrayInputStream(payload),
              Charset.forName("UTF-8")),
          clazz);
    }
    catch (Exception ex) {
      logger.error("{}", ex);
      return Optional.empty();
    }
    return Optional.of(result);
  }

  public <V> Optional<String> marshal(V object) {
    try {
      return Optional.of(mapper.writeValueAsString(object));
    } catch (JsonProcessingException e) {
      logger.error("{}", e);
    }
    return Optional.empty();
  }
}
