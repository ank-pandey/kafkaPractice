/**
 * 
 */
package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learnkafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Ankit.Pandey
 *
 */

@Component
@Slf4j
public class LibraryEventConsumer {
	
	@Autowired
	private LibraryEventsService libraryEventsService;

	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		
		log.info("ConsumerRecord : {} ", consumerRecord);
		libraryEventsService.processLibraryEvent(consumerRecord);
	}
}
