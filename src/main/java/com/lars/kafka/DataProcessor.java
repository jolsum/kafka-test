package com.lars.kafka;

import com.lars.kafka.model.Request;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

public class DataProcessor {

  private static final String IN_TOPIC = System.getenv("Topic.unprocessed");
  private static final String OUT_TOPIC = System.getenv("Topic.processed");

  public void run() {

    Properties props = KafkaUtil.getProperties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Processor");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RequestSerde.class.getName());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "5");

    StreamsBuilder builder = new StreamsBuilder();

    builder.<Long, Request>stream(IN_TOPIC).mapValues(this::process).to(OUT_TOPIC);

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();
  }

  private Request process(Request request) {
    System.out.println("Processed request " + request.getNum());
    return request.toBuilder().setProcessed(true).build();
  }

  public static void main(String[] args) {
    try {
      new DataProcessor().run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
