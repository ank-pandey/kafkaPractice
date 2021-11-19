package com.learnkafka.domain;

import org.springframework.lang.NonNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Ankit.Pandey
 *
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
	
	@NonNull
	private Integer bookId;
	@NonNull
	private String bookName;
	@NonNull
	private String bookAuthor;

}
