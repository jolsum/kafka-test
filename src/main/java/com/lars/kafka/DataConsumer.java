package com.lars.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.lars.kafka.model.Request;

public class DataConsumer {

	private static final String TOPIC = "1s6dym7u-job-unidentified";

	private final Object lock = new Object();
	private int received = 0;
	private long time = 0;

	public void run() {
		Executors.newSingleThreadScheduledExecutor()
				.scheduleWithFixedDelay(this::printStatus, 0, 5, TimeUnit.SECONDS);

		consume();
	}

	private void printStatus() {
		try {
			synchronized (lock) {
				if (received > 0) {
					System.out.println("Received " + received + ", avg delay: " + (time / received) + " ms");
					received = 0;
					time = 0;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void consume() {

		Properties properties = KafkaUtil.getProperties();
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Consumer");

		try (KafkaConsumer<Long, Request> consumer = new KafkaConsumer<>(properties)) {
			consumer.subscribe(Arrays.asList(TOPIC));
			while (true) {
				ConsumerRecords<Long, Request> records = consumer.poll(1000);
				for (ConsumerRecord<Long, Request> record : records) {

					long now = System.currentTimeMillis();

					System.out.println(record.value().getNum());

					synchronized (lock) {
						++received;
						time += now - record.key();
					}
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			new DataConsumer().run();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
