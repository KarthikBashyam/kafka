package com.demo.joins;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Kafka Joins Demo.
 * 
 * @author Karthik
 *
 */
public class KafkaJoinsDemoMain {

	public static void main(String[] args) {

		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transaction-stream");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		StreamsBuilder builder = new StreamsBuilder();
		
		Deserializer<Product> productDes = new ProductDeserializer(); 
		Serializer<Product> productSer = new ProductSerializer(); 
		Deserializer<ProductSalesInfo> productSalesDes = new ProductSalesInfoDeserializer(); 
		Serializer<ProductSalesInfo> productSalesSer = new ProductSalesInfoSerializer(); 
		
		Serde<Product> productSerde = Serdes.serdeFrom(productSer,productDes);
		Serde<ProductSalesInfo> productSalesSerde = Serdes.serdeFrom(productSalesSer, productSalesDes);
		
		KStream<String, Product> productsStream = builder.stream("products", Consumed.with(Serdes.String(), productSerde));
		KStream<String, ProductSalesInfo> productsSalesStream = builder.stream("products-sales", Consumed.with(Serdes.String(),productSalesSerde));
		
		KStream<String, String> productCountStream = productsStream.outerJoin(productsSalesStream, 
								(product, sales) -> {	return computeSales(sales);}, 
								JoinWindows.of(TimeUnit.MINUTES.toMinutes(60)),
								Joined.with(Serdes.String(), productSerde, productSalesSerde)								
								);

		productCountStream.foreach((key, value) -> System.out.println("Key: " + key + ", Product Sales:"+ value));

		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);

		kafkaStreams.cleanUp();
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
		System.out.println("============== JOINS STREAMS STARTED ====================");
	}

	private static String computeSales(ProductSalesInfo sales) {
		if (sales != null) {
			return sales.getCount().toString();
		} else {
			return "0";
		}
	}

}
