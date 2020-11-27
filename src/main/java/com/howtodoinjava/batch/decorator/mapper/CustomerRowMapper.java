package com.howtodoinjava.batch.decorator.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;
import com.howtodoinjava.batch.decorator.model.Customer;

public class CustomerRowMapper implements RowMapper<Customer> {

	@Override
	public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
		
		
		Customer customer=new Customer();
		
		customer.setId(rs.getLong("id"));
		customer.setFirstName(rs.getString("firstName"));
		customer.setLastName(rs.getString("lastName"));
		customer.setBirthdate(rs.getString("birthdate"));
		
		return customer;
	}
}
