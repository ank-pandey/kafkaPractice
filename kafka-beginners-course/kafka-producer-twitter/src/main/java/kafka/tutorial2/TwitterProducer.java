package kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	private String consumerKey = "UNWRf3fF0SCKuq9LmxT6vDbwk";
	private String consumerSecret = "9VrFrpbVvUtEDCLoEqppx312OhEqTggfz87YEj7LzJWY3yJUfy";
	private String token = "225223922-8DcJaZoENt4IPS65py1RdnF2vKBsJlboOKvUBhL1";
	private String secret = "6vZCtFv3zbF3MBt4nypzPkxeiiJTpEbL3jGbm11aT1LUl";
	private List<String> terms = Lists.newArrayList("bitcoin", "usa", "cricket");

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();

	}

	public void run() {

		logger.info("Setup");

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {

			logger.info("stopping appliccation....");
			logger.info("shutting down client from twitter...");
			client.stop();
			logger.info("closing producer");
			producer.close();
			logger.info("done!");
		}));
		// loop to send tweets to kafka

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				// something(msg);
				// profit();
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							logger.error("something bad happened", e);
						}
					}

				});
			}
		}
		logger.info("End of application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// .eventMessageQueue(eventQueue);
		// optional: use this if you want to process client events

		Client hosebirdClient = builder.build();

		return hosebirdClient;
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		// create producer properties
		String bootstrapServers = "127.0.0.1:9092";
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create safe producer
		prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		
		//high throughput producer (at the expense of a bit of latency and CPU usage)
		prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

		// create producers
		return new KafkaProducer<String, String>(prop);
	}

}
