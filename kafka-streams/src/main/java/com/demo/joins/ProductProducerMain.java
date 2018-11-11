package com.demo.joins;

import java.math.BigDecimal;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProductProducerMain {

	private static final String TOPIC_NAME = "products";

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProductSerializer.class.getName());
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, 3);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		Producer<String, Product> producer = new KafkaProducer<>(properties);

		for (int i = 1; i <= 2; i++) {
			producer.send(buildProduct());
		}
		producer.close();

	}

	private static ProducerRecord<String, Product> buildProduct() {
		Product product = new Product(10l, "BANANA", BigDecimal.valueOf(2500l));
		ProducerRecord<String, Product> producerRecord = new ProducerRecord<String, Product>(TOPIC_NAME, "BANANA",product);
		System.out.println("Product :" + product.getName());
		return producerRecord;
	}

}
