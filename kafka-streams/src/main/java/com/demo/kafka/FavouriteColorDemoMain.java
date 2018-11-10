package com.demo.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class FavouriteColorDemoMain {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-world");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> messagesStream = builder.stream("messages");

		KStream<String, String> userColorStream = messagesStream
				.selectKey((key, value) -> value.split(",")[0].toLowerCase())
				.mapValues((key, value) -> value.split(",")[1].toLowerCase());

		userColorStream.to("user-color-table");

		KTable<String, String> userColorTable = builder.table("user-color-table");
		KTable<String, Long> favColorTable = userColorTable.groupBy((user, color) -> new KeyValue<>(color, color))
				.count();

		favColorTable.toStream().foreach((key, value) -> System.out.println(key + " : " + value));

		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
		kafkaStreams.cleanUp();
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

	}

}