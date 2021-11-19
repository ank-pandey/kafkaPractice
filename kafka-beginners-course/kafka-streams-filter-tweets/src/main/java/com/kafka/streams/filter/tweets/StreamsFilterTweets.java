package com.kafka.streams.filter.tweets;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {

	public static void main(String[] args) {

		// create properties
		Properties prop = new Properties();
		prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());

		// create a topology
		StreamsBuilder builder = new StreamsBuilder();

		// input topic
		KStream<String, String> inputTopic = builder.stream("twitter_tweets");

		KStream<String, String> filteredStream = inputTopic.filter(
				// filter for tweets which has a user of over 10000 followers
				(k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000);

		filteredStream.to("important_tweets");

		// build a topology
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), prop);

		// start our streams application
		kafkaStreams.start();
	}

	private static Integer extractUserFollowersInTweet(String tweetJson) {
		try {
			return JsonParser.parseString(tweetJson).getAsJsonObject().get("user").getAsJsonObject()
					.get("followers_count").getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}
}