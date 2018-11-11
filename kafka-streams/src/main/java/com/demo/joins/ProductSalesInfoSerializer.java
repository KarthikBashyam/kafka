package com.demo.joins;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductSalesInfoSerializer implements Serializer<ProductSalesInfo> {
	
	private ObjectMapper objectMapper = null;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		objectMapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(String topic, ProductSalesInfo data) {
		try {
			objectMapper = new ObjectMapper();
			return objectMapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			System.out.println("Failed to serialze the object :" + e.getMessage());
		}
		return null;
	}

	@Override
	public void close() {

	}

}
