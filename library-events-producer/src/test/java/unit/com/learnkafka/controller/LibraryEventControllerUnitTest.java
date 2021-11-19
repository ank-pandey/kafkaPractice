/**
 * 
 */
package com.learnkafka.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;

/**
 * @author Ankit.Pandey
 *
 */
@ExtendWith(SpringExtension.class)
@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

	@Autowired
	MockMvc mockMvc;
	
	ObjectMapper mapper = new ObjectMapper();
	
	@MockBean
	LibraryEventProducer libraryEventProducer;
	
	@Test
	void postLibraryEvent() throws Exception {
		
		//given
		Book book = Book.builder().bookId(123).bookName("test").bookAuthor("ankit").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String json = mapper.writeValueAsString(libraryEvent);
		//doNothing().when(libraryEventProducer).sendLibraryEventAsyncApproach2(isA(LibraryEvent.class));
		when(libraryEventProducer.sendLibraryEventAsyncApproach2(isA(LibraryEvent.class))).thenReturn(null);
		//when
		mockMvc.perform(post("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().isCreated());
	}
	
	@Test
	void postLibraryEvent_4xx() throws Exception {
		
		//given
		//Book book = Book.builder().bookId(123).bookName(null).bookAuthor(null).build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(new Book()).build();
		String json = mapper.writeValueAsString(libraryEvent);
		//doNothing().when(libraryEventProducer).sendLibraryEventAsyncApproach2(isA(LibraryEvent.class));
		when(libraryEventProducer.sendLibraryEventAsyncApproach2(isA(LibraryEvent.class))).thenReturn(null);
		//expect
		String expectedErrorMessage = "";
		mockMvc.perform(post("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().is4xxClientError())
						.andExpect(content().string(expectedErrorMessage));
	}
	
	@Test
	void updateLibraryEvent() throws Exception {
		
		//given
		Book book = Book.builder().bookId(456).bookName("test").bookAuthor("ankit").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(123).book(book).build();
		String json = mapper.writeValueAsString(libraryEvent);
		//doNothing().when(libraryEventProducer).sendLibraryEventAsyncApproach2(isA(LibraryEvent.class));
		when(libraryEventProducer.sendLibraryEventAsyncApproach2(isA(LibraryEvent.class))).thenReturn(null);
		//when
		mockMvc.perform(put("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().isOk());
	}
	
	@Test
	void updateLibraryEvent_withNullLibraryEventId() throws Exception {
		
		//given
		Book book = Book.builder().bookId(456).bookName("test").bookAuthor("ankit").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String json = mapper.writeValueAsString(libraryEvent);
		//doNothing().when(libraryEventProducer).sendLibraryEventAsyncApproach2(isA(LibraryEvent.class));
		when(libraryEventProducer.sendLibraryEventAsyncApproach2(isA(LibraryEvent.class))).thenReturn(null);
		//expect
		String expectedErrorMessage = "Please pass the LibraryEventId";
		mockMvc.perform(put("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().isBadRequest())
						.andExpect(content().string(expectedErrorMessage));
	}
}
