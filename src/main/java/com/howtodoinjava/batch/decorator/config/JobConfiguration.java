package com.howtodoinjava.batch.decorator.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.howtodoinjava.batch.decorator.aggregator.CustomLineAggregator;
import com.howtodoinjava.batch.decorator.classifier.CustomerClassifier;
import com.howtodoinjava.batch.decorator.mapper.CustomerRowMapper;
import com.howtodoinjava.batch.decorator.model.Customer;

@Configuration
public class JobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public JdbcPagingItemReader<Customer> reader() {
		
		
//		JdbcCursorItemReader you read all the records from the database, 
//		you hold them in memory and you go up and down the result set by using a cursor 
//		(a cursor points to the current row of data). When a user changes page you don't read the data from the database again.
//
//		JdbcPagingItemReader you read chunks of records from the database. 
//		But when the user changes page you issue another query and read the data again.

//		First approach: You consume more memory but it's faster.
//		Second approach: You consume less memory but it's slower.

		// reading database records using JDBC in a paging fashion
		
		//JdbcPagingItemReader – This bean help to read database records using JDBC using pagination fashion
		
	//	Spring Batch decorators to classify the data to write to the multiple destinations.
		//This is very much needed when you work in enterprise architecture to pass/share data to multiple systems.

		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		reader.setDataSource(this.dataSource);
		reader.setFetchSize(1000);
		reader.setRowMapper(new CustomerRowMapper());
		

		// Sort Keys
		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("id", Order.DESCENDING);

		// 	MySQL implementation of a PagingQueryProvider using database specific features.

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from customer");
		queryProvider.setSortKeys(sortKeys);
		reader.setQueryProvider(queryProvider);
		return reader;
	}
	
   // private Resource outputResource1 = new FileSystemResource("output/CustomerOutput_EvenId.csv");
 //   private Resource outputResource2 = new FileSystemResource("output/CustomerOutput_OddId.csv");


	@Bean
	public FlatFileItemWriter<Customer> evenItemWriter() throws Exception {

		String customerOutputPath = File.createTempFile("customerOutputEven", ".out").getAbsolutePath();
		System.out.println(">> Output Path = " + customerOutputPath);
		FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
		writer.setLineAggregator(new CustomLineAggregator());
		writer.setResource(new FileSystemResource(customerOutputPath));
		writer.afterPropertiesSet();
		return writer;
	}
	
	@Bean
	public FlatFileItemWriter<Customer> oddItemWriter() throws Exception {

		String customerOutputPath = File.createTempFile("customerOutputOdd", ".out").getAbsolutePath();
		System.out.println(">> Output Path = " + customerOutputPath);
		FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
		writer.setLineAggregator(new CustomLineAggregator());
		writer.setResource(new FileSystemResource(customerOutputPath));
		writer.afterPropertiesSet();
		return writer;
	}
	
	//ClassifierCompositeItemWriter is one of the decorators
	//ClassifierCompositeItemWriter calls one of a collection of ItemWriter implementations for each item, 
	//based on a pattern implemented through the provided Classifier. 
	//CustomerClassifier is classifier class.
	@Bean
	public ClassifierCompositeItemWriter<Customer> classifierCustomerCompositeItemWriter() throws Exception {
		ClassifierCompositeItemWriter<Customer> compositeItemWriter = new ClassifierCompositeItemWriter<>();
		compositeItemWriter.setClassifier(new CustomerClassifier(evenItemWriter(), oddItemWriter()));
		return compositeItemWriter;
	}

	@Bean
	public Step step1() throws Exception {
		return stepBuilderFactory.get("step1")
				.<Customer, Customer>chunk(10)
				.reader(reader())
				.writer(classifierCustomerCompositeItemWriter())
				.stream(evenItemWriter())
				.stream(oddItemWriter())
				.build();
	}

	@Bean
	public Job job() throws Exception {
		return jobBuilderFactory.get("job")
				.start(step1())
				.build();
	}

}