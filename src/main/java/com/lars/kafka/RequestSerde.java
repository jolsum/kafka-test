package com.lars.kafka;

import com.lars.kafka.model.Request;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class RequestSerde implements Serde<Request> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<Request> serializer() {
    return new RequestSerializer();
  }

  @Override
  public Deserializer<Request> deserializer() {
    return new RequestDeserializer();
  }
}
