package com.demo.bank.balance;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Bank Balance Stream Processor.
 * 
 * @author Karthik Bashyam
 *
 */
public class BankBalanceStreamProcessor {
	
	public static void main(String[] args) {
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transaction-stream");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
		
		Serializer<JsonNode> jsonSerializer = new JsonSerializer();
		Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
		
		Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		
		System.out.println("============= BANK BALANCE PROCESSOR ===================");
		
		StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, JsonNode> transactionStream = builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde));
		
		ObjectNode initObject = JsonNodeFactory.instance.objectNode();
		initObject.put("count", 0);
		initObject.put("amount", 0);
		initObject.put("time", Instant.ofEpochMilli(0L).toEpochMilli());
		
		KTable<String, JsonNode> balanceTable = transactionStream.groupByKey()
						 .aggregate(() -> initObject, 
								    (key, value, aggregator) -> {return calculate(value, aggregator);},
								    Materialized.with(Serdes.String(), jsonSerde)
								   );
		
		balanceTable.toStream().foreach((key, value) -> System.out.println(key + " : "+value.get("amount")));
		
			
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
				
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
						
		
	}

	private static ObjectNode calculate(JsonNode value, JsonNode aggregator) {
		int balance = aggregator.get("amount").asInt() + value.get("amount").asInt();

		ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
		objectNode.put("amount", balance);
		objectNode.put("count" , aggregator.get("count").asInt() + 1);
		
		return objectNode;
	}

}
