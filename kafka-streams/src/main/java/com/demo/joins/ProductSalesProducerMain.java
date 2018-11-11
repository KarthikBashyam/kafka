package com.demo.joins;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProductSalesProducerMain {

	private static final String TOPIC_NAME = "products-sales";

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProductSalesInfoSerializer.class.getName());
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, 3);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		Producer<String, ProductSalesInfo> producer = new KafkaProducer<>(properties);

		for (int i = 1; i <= 1; i++) {
			producer.send(buildProductSales());
		}
		producer.close();

	}

	private static ProducerRecord<String, ProductSalesInfo> buildProductSales() {
		ProductSalesInfo salesInfo = new ProductSalesInfo(0L, "BANANA", "Walmart", 28800L);
		ProducerRecord<String, ProductSalesInfo> producerRecord = new ProducerRecord<>(TOPIC_NAME,"BANANA",salesInfo);
		return producerRecord;
	}

}
