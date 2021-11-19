/**
 * 
 */
package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Ankit.Pandey
 *
 */

//@Component
@Slf4j
public class LibraryEventConsumerManualOffset implements AcknowledgingMessageListener<Integer, String>{

	@Override
	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
		log.info("ConsumerRecord : {} ", consumerRecord);
		acknowledgment.acknowledge();
	}
}
