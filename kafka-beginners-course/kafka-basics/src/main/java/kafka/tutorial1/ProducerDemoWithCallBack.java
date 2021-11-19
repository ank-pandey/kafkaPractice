package kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBack {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
		String bootstrapServers = "127.0.0.1:9092";
		// create producer properties
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producers
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
		for (int i = 0; i < 10; i++) {
			// create producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
					"hello world" + Integer.toString(i));

			// send data asynchronously
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception e) {
					// executes every time a record is successfully sent or an exception is thrown

					if (e == null) {
						// the record was sent successfully
						logger.info("Recived new metadata: \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
								+ metadata.timestamp());

					} else {
						logger.error("Error while producing", e);
					}

				}
			});
		}

		// flush data
		producer.flush();

		// flush and close the producer
		producer.close();
	}

}
