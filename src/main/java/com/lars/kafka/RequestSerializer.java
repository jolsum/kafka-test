package com.lars.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.lars.kafka.model.Request;

public class RequestSerializer implements Serializer<Request> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, Request data) {
		return data.toByteArray();
	}

	@Override
	public void close() {
	}

}
