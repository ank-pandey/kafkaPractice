package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroupsDemo {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerGroupsDemo.class.getName());
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fifth-application";
		String topic = "first_topic";
		Properties prop = new Properties();

		// create consume configs
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

		// subscribe consumer to any topic
		// consumer.subscribe(Collections.singleton(topic));
		consumer.subscribe(Arrays.asList(topic));

		// poll for new data
		while (true) {
			// consumer.poll(100);
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

			for (ConsumerRecord<String, String> record : records) {

				logger.info("Key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition:" + record.partition() + ", Offset: " + record.offset());
			}
		}
	}
}
