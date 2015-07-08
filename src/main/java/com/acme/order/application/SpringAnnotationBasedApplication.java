package com.acme.order.application;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.acme.order.Customer;
import com.acme.order.pizza.PizzaOrderService;
import com.acme.order.pizza.PizzaType;

@Slf4j
@Configuration
@ComponentScan("com.acme.order")
public class SpringAnnotationBasedApplication {

	public static void main(String[] args) {
		log.info("Spring XML based application");

		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAnnotationBasedApplication.class);
		PizzaOrderService orderService = ctx.getBean(PizzaOrderService.class);

		Customer customer1 = new Customer(null, "John Smith", "john@smith.com", "Lodz, Jaracza 74");
		Customer customer2 = new Customer(null, "Jan Kowalski", "jan@kowalski.pl", "Lodz, Piotrkowska 100");

		printDataSourceStats(ctx);

		String orderId1 = orderService.createOrder(customer1, PizzaType.LARGE);
		String orderId2 = orderService.createOrder(customer2, PizzaType.SMALL);

		printDataSourceStats(ctx);

		log.info("Unprocessed orders:{}", orderService.fetchUnprocessed());
		log.info("Delivered orders:{}", orderService.fetchDelivered());

		orderService.deliverOrder(orderId1);
		log.info("Delivered orders:{}", orderService.fetchDelivered());
		orderService.cancelOrder(orderId2);
		log.info("Delivered orders:{}", orderService.fetchDelivered());
		log.info("Cancelled orders:{}", orderService.fetchCancelled());
		log.info("Unprocessed orders:{}", orderService.fetchUnprocessed());

		printDataSourceStats(ctx);

	}

	@Bean(destroyMethod = "close")
	public DataSource dataSource() {
		// http://commons.apache.org/proper/commons-dbcp/configuration.html
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setUrl("jdbc:mysql://localhost:3306/pizza-tutorial");
		ds.setUsername("dbuser");
		ds.setPassword("dbpass");

		ds.setInitialSize(5);

		return ds;
	}

	public static void printDataSourceStats(ApplicationContext ctx) {

		BasicDataSource bds = (BasicDataSource) ctx.getBean(DataSource.class);
		log.info("NumActive: " + bds.getNumActive());
		log.info("NumIdle: " + bds.getNumIdle());
	}
}
