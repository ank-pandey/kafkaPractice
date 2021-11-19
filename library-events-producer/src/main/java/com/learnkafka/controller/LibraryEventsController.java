package com.learnkafka.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Ankit.Pandey
 *
 */

@RestController
@Slf4j
public class LibraryEventsController {

	@Autowired
	LibraryEventProducer libraryEventProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
			throws Exception {
		// invoke kafka producer
		// log.info("Before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		// libraryEventProducer.sendLibraryEvent(libraryEvent);
		// SendResult<Integer, String> sendResult =
		// libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		libraryEventProducer.sendLibraryEventAsyncApproach2(libraryEvent);
		// log.info("sendResult is {}", sendResult.toString());
		log.info("After sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
			throws JsonProcessingException {

		if(null == libraryEvent.getLibraryEventId()) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
		}
		
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEventAsyncApproach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
}
