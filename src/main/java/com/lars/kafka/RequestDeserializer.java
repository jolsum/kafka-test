package com.lars.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.lars.kafka.model.Request;

public class RequestDeserializer implements Deserializer<Request> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public Request deserialize(String topic, byte[] data) {
		try {
			return Request.parseFrom(data);
		}
		catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void close() {
	}

}
