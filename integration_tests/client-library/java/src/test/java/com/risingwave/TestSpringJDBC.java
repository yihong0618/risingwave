package com.risingwave;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class TestSpringJDBC {
    static JdbcTemplate configJdbcTemplate() {
        DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql://risingwave-standalone:4566/dev", "root", "");

        return new JdbcTemplate(dataSource);
    }

    @Test
    public void TestJDBCTemplate() throws Exception {
        JdbcTemplate jdbcTemplate = configJdbcTemplate();

        jdbcTemplate.execute("DROP TABLE IF EXISTS customers");
        jdbcTemplate.execute("CREATE TABLE customers(" +
                "id INTEGER PRIMARY KEY, first_name VARCHAR, last_name VARCHAR)");

        // Split up the array of whole names into an array of first/last names
        List<Object[]> splitUpNames = Arrays.asList("John Woo", "Jeff Dean", "Josh Bloch", "Josh Long").stream()
                .map(name -> name.split(" "))
                .collect(Collectors.toList());

        // Use a Java 8 stream to print out each tuple of the list
        splitUpNames.forEach(
                name -> System.out.println(String.format("Inserting customer record for %s %s", name[0], name[1])));

        // Uses JdbcTemplate's batchUpdate operation to bulk load data
        jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name) VALUES (?,?)", splitUpNames);

        jdbcTemplate.query(
                "SELECT id, first_name, last_name FROM customers WHERE first_name = ?",
                (rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"),
                        rs.getString("last_name")),
                "Josh")
                .forEach(customer -> System.out.println(customer.toString()));
    }
}
