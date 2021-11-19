/**
 * 
 */
package com.learnkafka.producer;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

/**
 * @author Ankit.Pandey
 *
 */

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

	@InjectMocks
	LibraryEventProducer libraryEventProducer;

	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Spy
	ObjectMapper objectMapper = new ObjectMapper();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void sendLibraryEventAsyncApproach2_failure()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book book = Book.builder().bookId(123).bookName("test").bookAuthor("ankit").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		SettableListenableFuture future = new SettableListenableFuture();
		future.setException(new RuntimeException("Exception Calling Kafka"));
		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
		// when
		Assertions.assertThrows(Exception.class,
				() -> libraryEventProducer.sendLibraryEventAsyncApproach2(libraryEvent).get());
		// then
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void sendLibraryEventAsyncApproach2_success()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book book = Book.builder().bookId(123).bookName("test").bookAuthor("ankit").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String record = objectMapper.writeValueAsString(libraryEvent);

		SettableListenableFuture future = new SettableListenableFuture();
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342,
				System.currentTimeMillis(), 1, 2);
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events",
				libraryEvent.getLibraryEventId(), record);
		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
		future.set(sendResult);
		// when
		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
		ListenableFuture<SendResult<Integer, String>> futureObject = libraryEventProducer
				.sendLibraryEventAsyncApproach2(libraryEvent);

		// then
		SendResult<Integer, String> result = futureObject.get();
		assert result.getRecordMetadata().partition() == 1;
	}

}
