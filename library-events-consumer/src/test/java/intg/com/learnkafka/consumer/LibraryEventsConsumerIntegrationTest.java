package com.learnkafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry endPointRegistery;
	
	@SpyBean
	LibraryEventConsumer libraryEventConsumerSpy;
	
	@SpyBean
	LibraryEventsService libraryEventsServiceSpy;
	
	@Autowired
	LibraryEventsRepository repo;
	
	@Autowired
	ObjectMapper mapper;
	
	@BeforeEach
	void setUp() {
		for(MessageListenerContainer messageListenerContainer : endPointRegistery.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}
	
	@AfterEach
	void tearDown() {
		repo.deleteAll();
	}
	
	@SuppressWarnings("unchecked")
	//@Test
	void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
		//given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"test\",\"bookAuthor\":\"ankit\"}}";
		kafkaTemplate.sendDefault(json).get();
		
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEventsList = (List<LibraryEvent>) repo.findAll();
		assert libraryEventsList.size() == 1;
		libraryEventsList.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId() != null;
			assertEquals(123, libraryEvent.getBook().getBookId());
		});
	}
	
	//@Test
	void publishUpdateLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
		//given
		String json = "{\"libraryEventId\":null,\"eventStatus\":\"ADD\",\"book\":{\"bookId\":123,\"bookName\":\"test\",\"bookAuthor\":\"ankit\"}}";
		LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repo.save(libraryEvent);
		
		//publish the update Library Event
		Book updatedBook = Book.builder().bookId(123).bookName("test123").bookAuthor("ank_pandey").build();
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		
		String updatedJson = mapper.writeValueAsString(libraryEvent);
		
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();
		
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		//then
		
		//verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		//verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		LibraryEvent persistedLibraryEvent = repo.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals("test123", persistedLibraryEvent.getBook().getBookName());	
	}
	
	@SuppressWarnings("unchecked")
	//@Test
    void publishModifyLibraryEvent_InValid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 123;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"test\",\"bookAuthor\":\"ankit\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = repo.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());
    }
	
	@SuppressWarnings("unchecked")
	//@Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"test\",\"bookAuthor\":\"ankit\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }
	
	@SuppressWarnings("unchecked")
	@Test
    void publishModifyLibraryEvent_000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"test\",\"bookAuthor\":\"ankit\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        //verify(libraryEventConsumerSpy, atLeast(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(4)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1)).handleRecovery(isA(ConsumerRecord.class));
    }


}
