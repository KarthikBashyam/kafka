package com.demo.joins;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductSalesInfoDeserializer implements Deserializer<ProductSalesInfo> {

	private ObjectMapper objectMapper = null;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public ProductSalesInfo deserialize(String topic, byte[] data) {
		try {
			objectMapper = new ObjectMapper();
			return objectMapper.readValue(data, new TypeReference<ProductSalesInfo>() {
			});
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to deserialize Product class: "+e.getMessage());
		}
		return null;
	}

	@Override
	public void close() {

	}

}
