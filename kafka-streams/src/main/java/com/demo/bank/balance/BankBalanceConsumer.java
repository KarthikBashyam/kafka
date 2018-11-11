package com.demo.bank.balance;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Bank Balance Kafka Consumer
 * 
 * @author Karthik
 *
 */
public class BankBalanceConsumer {

	public static void main(String[] args) {

		Properties config = new Properties();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.CLIENT_ID_CONFIG, "bank-balance-consumer");
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "bank-consumer-gp1");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(config);
		consumer.subscribe(Collections.singletonList("bank-transactions"));

		System.out.println("============== BALANCE CONSUMER =================");

		while (true) {
			ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.ofMillis(2));

			for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
				System.out.println(record.value().get("name") + " : " + record.value().get("amount"));
			}
		}

	}

}
