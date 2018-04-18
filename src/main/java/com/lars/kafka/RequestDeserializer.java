package com.lars.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.lars.kafka.model.Request;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.serialization.Deserializer;

public class RequestDeserializer implements Deserializer<Request> {

  public static final AtomicLong bytes = new AtomicLong();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Request deserialize(String topic, byte[] data) {
    try {
      bytes.addAndGet(data.length);
      return Request.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void close() {}

}
