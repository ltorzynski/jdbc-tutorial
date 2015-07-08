package com.acme.order;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.acme.order.pizza.PizzaOrder;
import com.acme.order.pizza.PizzaType;

@Repository
@Primary
public class SpringJdbcTemplateOrderRepository implements OrderRepository {

	private JdbcTemplate jdbcTemplate;

	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}

	@Override
	public String save(PizzaOrder order) {
		int customerId = insertOrUpdateCustomer(order.getCustomer());
		int orderId = insertOrUpdateOrder(order, customerId);
		return Integer.valueOf(orderId)
						.toString();
	}

	private int insertOrUpdateOrder(PizzaOrder order, int customerId) {
		final String INSERT_SQL = "INSERT INTO order_t (customer_id,status,type,estimatedDeliveryTime,finishTime) VALUES (:customerId,:status,:type,:estimatedDeliveryTime,:finishTime)";
		final String UPDATE_SQL = "UPDATE order_t SET customer_id=?,status=?,type=?,estimatedDeliveryTime=?,finishTime=? WHERE id = ?";

		String SQL = INSERT_SQL;
		boolean insert = true;
		if (order.getId() != null) {
			SQL = UPDATE_SQL;
			insert = false;
		}

		if (insert) {
			KeyHolder keyHolder = new GeneratedKeyHolder();

			namedParameterJdbcTemplate.update(SQL, new MapSqlParameterSource(new HashMap<String, Object>() {
				{
					put("customerId", customerId);
					put("status", order.getState()
										.name());
					put("type", order.getPizzaType()
										.name());
					put("estimatedDeliveryTime", order.getEstimatedTime());
					put("finishTime", order.getFinishTime());
				}
			}), keyHolder);

			return keyHolder.getKey()
							.intValue();
		} else {
			jdbcTemplate.update(SQL, customerId, order.getState()
														.name(), order.getPizzaType()
																		.name(), order.getEstimatedTime(),
					order.getFinishTime(), order.getId());
			return Integer.valueOf(order.getId());
		}

	}

	private int insertOrUpdateCustomer(Customer customer) {
		final String INSERT_SQL = "INSERT INTO customer_t (name,email,address) VALUES (:name,:email,:address)";
		final String UPDATE_SQL = "UPDATE customer_t SET name=?,email=?,address=? WHERE id = ?";

		String SQL = INSERT_SQL;
		boolean insert = true;
		if (customer.getId() != null) {
			SQL = UPDATE_SQL;
			insert = false;
		}
		if (insert) {
			KeyHolder keyHolder = new GeneratedKeyHolder();

			namedParameterJdbcTemplate.update(SQL, new BeanPropertySqlParameterSource(customer), keyHolder);

			return keyHolder.getKey()
							.intValue();
		} else {
			jdbcTemplate.update(SQL, customer.getName(), customer.getEmail(), customer.getAddress(), customer.getId());
		}
		return Integer.valueOf(customer.getId());
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub

	}

	@Override
	public PizzaOrder get(String pizzaOrderId) {
		final String SQL = "SELECT o.id as order_id,o.customer_id as customer_id,o.status,o.type,o.estimatedDeliveryTime,"
				+ "o.finishTime,c.name,c.email,c.address from order_t o,customer_t c where o.customer_id = c.id and o.id = ?";

		return jdbcTemplate.queryForObject(SQL, (rs, rowNum) -> {
			return buildOrder(rs);
		}, pizzaOrderId);
	}

	@Override
	public List<PizzaOrder> findAll() {
		final String SQL = "SELECT o.id as order_id,o.customer_id as customer_id,o.status,o.type,o.estimatedDeliveryTime,"
				+ "o.finishTime,c.name,c.email,c.address from order_t o,customer_t c where o.customer_id = c.id";

		return jdbcTemplate.query(SQL, (rs, rowNum) -> {
			return buildOrder(rs);
		});
	}

	@Override
	public List<PizzaOrder> findByOrderStatus(OrderStatus orderStatus) {
		final String SQL = "SELECT o.id as order_id,o.customer_id as customer_id,o.status,o.type,o.estimatedDeliveryTime,"
				+ "o.finishTime,c.name,c.email,c.address from order_t o,customer_t c where o.customer_id = c.id and o.status = ?";

		return jdbcTemplate.query(SQL, (rs, rowNum) -> {
			return buildOrder(rs);
		}, orderStatus.name());
	};

	private PizzaOrder buildOrder(ResultSet rs) throws SQLException {
		PizzaType pizzaType = PizzaType.valueOf(rs.getString("type"));
		String orderId = String.valueOf(rs.getInt("order_id"));
		Customer customer = new Customer(String.valueOf(rs.getInt("customer_id")), rs.getString("name"),
				rs.getString("email"), rs.getString("address"));
		PizzaOrder order = new PizzaOrder(customer, pizzaType);
		order.setId(orderId);
		order.withEstimatedTime(rs.getTimestamp("estimatedDeliveryTime"));
		order.setFinishTime(rs.getTimestamp("finishTime"));
		return order;
	}

}
