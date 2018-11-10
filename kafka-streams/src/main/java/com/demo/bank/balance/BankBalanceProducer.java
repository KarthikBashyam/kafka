package com.demo.bank.balance;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BankBalanceProducer {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, 3);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		Producer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 1; i <= 2; i++) {
			
			try {
				producer.send(buildTransaction("john"));
				TimeUnit.MILLISECONDS.sleep(200);
				producer.send(buildTransaction("alice"));
				TimeUnit.MILLISECONDS.sleep(200);
				producer.send(buildTransaction("ajay"));
				TimeUnit.MILLISECONDS.sleep(200);		
			} catch(InterruptedException e) {
				break;
			}
		}
		producer.close();

	}

	private static ProducerRecord<String, String> buildTransaction(String name) {
		ObjectNode transaction = JsonNodeFactory.instance.objectNode();
		int amount = ThreadLocalRandom.current().nextInt(0, 100);
		transaction.put("name", name);
		transaction.put("amount", 100);
		transaction.put("time", Instant.now().toString());
		System.out.println(name + " : "+amount);
		return new ProducerRecord<String, String>("bank-transactions", name, transaction.toString());
	}

}
