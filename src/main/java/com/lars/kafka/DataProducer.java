package com.lars.kafka;

import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.lars.kafka.model.Request;

public class DataProducer {

	private static final String TOPIC = System.getenv("Topic.unprocessed");

	private final int QPS = 5;

	private void run() {
		Executors.newSingleThreadExecutor().execute(this::runProducer);
	}

	private void runProducer() {

		long sleepTime = 1000 / QPS;

		Properties properties = KafkaUtil.getProperties();
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer");

		try (Producer<Long, Request> producer = new KafkaProducer<>(properties)) {

			int i = 0;
			while (true) {
				Request request = Request.newBuilder()
						.setNum(++i)
						.build();

				ProducerRecord<Long, Request> record = new ProducerRecord<>(TOPIC,
						System.currentTimeMillis(), request);

				producer.send(record);

				Thread.sleep(sleepTime);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			new DataProducer().run();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
