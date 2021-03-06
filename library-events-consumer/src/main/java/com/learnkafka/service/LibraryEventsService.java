package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {
	
	@Autowired
	ObjectMapper objectMapper; 
	
	@Autowired
	private LibraryEventsRepository repo;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		
		log.info("libraryEvent : {} ", libraryEvent);
		
		if(libraryEvent.getLibraryEventType() != null && libraryEvent.getLibraryEventId()==000) {
			
			throw new RecoverableDataAccessException("Temporary Network Issue");
		}
		
		switch(libraryEvent.getLibraryEventType()) {
			
		case NEW:
			save(libraryEvent);
			break;
		
		case UPDATE:
			validate(libraryEvent);
			save(libraryEvent);
			break;
			
		default:
			log.info("Invalid Library Event Type");
		}
		
	}

	private void validate(LibraryEvent libraryEvent) {
		if(null == libraryEvent.getLibraryEventId()) {
			throw new IllegalArgumentException("Library Event Id is missing");
		}
		
		Optional<LibraryEvent> findById = repo.findById(libraryEvent.getLibraryEventId());
		
		if(! findById.isPresent()) {
			throw new IllegalArgumentException("Not a valid Library Event");
		}
		log.info("Validation is successful for the library Event: {} ", findById.get());
	}

	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repo.save(libraryEvent);
		log.info("Successfully Persisted the library event {} ", libraryEvent);
	}
	
	public void handleRecovery(ConsumerRecord<Integer, String> record) {
	
		ListenableFuture<SendResult<Integer, String>> result = kafkaTemplate.sendDefault(record.key(), record.value());
		
		result.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(record.key(), record.value(), result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(record.key(), record.value(), ex);
			}
		});
	}
	
	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending the Message and the exception is {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("Error in OnFailure: {}", throwable.getMessage());
		}
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message send successfully for the key : {} and the value is {}, partition is {}", key, value,
				result.getRecordMetadata().partition());
	}

}
