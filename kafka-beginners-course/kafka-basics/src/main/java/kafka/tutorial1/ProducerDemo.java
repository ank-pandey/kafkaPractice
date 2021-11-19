package kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		String bootstrapServers = "127.0.0.1:9092";
		//create producer properties
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create producers
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
		
		//create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world!");
		
		//send data asynchronously
		producer.send(record);
		
		//flush data
		producer.flush();
		
		//flush and close the producer
		producer.close();
	}

}
