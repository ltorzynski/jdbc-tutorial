package com.acme.order;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

import com.acme.order.pizza.PizzaOrder;
import com.acme.order.pizza.PizzaType;

@Slf4j
@Repository
@Primary
public class JdbcOrderRepository implements OrderRepository {

	@Autowired
	private DataSource dataSource;

	@Override
	public String save(PizzaOrder order) {
		try {
			int customerId = insertOrUpdateCustomer(order.getCustomer());
			int orderId = insertOrUpdateOrder(order, customerId);
			return Integer.valueOf(orderId)
							.toString();
		} catch (SQLException e) {
			log.error("Error while saving pizzaOrder", e);
			throw new RuntimeException("Error while saving pizzaOrder", e);
		}

	}

	private int insertOrUpdateOrder(PizzaOrder order, int customerId) throws SQLException {
		final String INSERT_SQL = "INSERT INTO order_t (customer_id,status,type,estimatedDeliveryTime,finishTime) VALUES (?,?,?,?,?)";
		final String UPDATE_SQL = "UPDATE order_t SET customer_id=?,status=?,type=?,estimatedDeliveryTime=?,finishTime=? WHERE id = ?";

		String SQL = INSERT_SQL;
		boolean insert = true;
		if (order.getId() != null) {
			SQL = UPDATE_SQL;
			insert = false;
		}

		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement statement = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
				statement.setInt(1, customerId);
				statement.setString(2, order.getState()
											.name());
				statement.setString(3, order.getPizzaType()
											.name());

				statement.setTimestamp(4, order.getEstimatedTime() != null ? new Timestamp(order.getEstimatedTime()
																								.getTime()) : null);
				statement.setTimestamp(5, order.getFinishTime() != null ? new Timestamp(order.getFinishTime()
																								.getTime()) : null);
				if (!insert)
					statement.setInt(6, Integer.valueOf(order.getId()));
				int id = statement.executeUpdate();

				try (ResultSet rs = statement.getGeneratedKeys()) {
					if (rs.next()) {
						id = rs.getInt(1);
					}

				}

				return insert ? id : Integer.valueOf(order.getId());
			}
		}
	}

	private int insertOrUpdateCustomer(Customer customer) throws SQLException {
		final String INSERT_SQL = "INSERT INTO customer_t (name,email,address) VALUES (?,?,?)";
		final String UPDATE_SQL = "UPDATE customer_t SET name=?,email=?,address=? WHERE id = ?";

		String SQL = INSERT_SQL;
		boolean insert = true;
		if (customer.getId() != null) {
			SQL = UPDATE_SQL;
			insert = false;
		}

		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement statement = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
				statement.setString(1, customer.getName());
				statement.setString(2, customer.getEmail());
				statement.setString(3, customer.getAddress());
				if (!insert)
					statement.setInt(4, Integer.valueOf(customer.getId()));
				int id = statement.executeUpdate();

				try (ResultSet rs = statement.getGeneratedKeys()) {
					if (rs.next()) {
						id = rs.getInt(1);
					}

				}

				return insert ? id : Integer.valueOf(customer.getId());
			}
		}
	}

	@Override
	public void rollback() {
		log.info("Fake db rollback...");

	}

	@Override
	public PizzaOrder get(String pizzaOrderId) {
		final String SQL = "SELECT o.id as order_id,o.customer_id as customer_id,o.status,o.type,o.estimatedDeliveryTime,"
				+ "o.finishTime,c.name,c.email,c.address from order_t o,customer_t c where o.customer_id = c.id and o.id = ?";

		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement statement = connection.prepareStatement(SQL)) {

				statement.setInt(1, Integer.valueOf(pizzaOrderId));

				try (ResultSet rs = statement.executeQuery()) {

					if (rs.next())
						return buildOrder(rs);
					return null;
				}
			}
		} catch (SQLException e) {
			log.error("Error while fetching order by id", e);
			throw new RuntimeException("Error while fetching order by id", e);
		}
	}

	@Override
	public List<PizzaOrder> findAll() {
		final String SQL = "SELECT o.id as order_id,o.customer_id as customer_id,o.status,o.type,o.estimatedDeliveryTime,"
				+ "o.finishTime,c.name,c.email,c.address from order_t o,customer_t c where o.customer_id = c.id";

		List<PizzaOrder> orders = new ArrayList<>();

		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(SQL)) {
				while (rs.next()) {
					orders.add(buildOrder(rs));

				}
				return orders;
			}
		} catch (SQLException e) {
			log.error("Error while fetching all orders", e);
			throw new RuntimeException("Error while fetching all orders", e);
		}
	}

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

	@Override
	public List<PizzaOrder> findByOrderStatus(OrderStatus orderStatus) {
		final String SQL = "SELECT o.id as order_id,o.customer_id as customer_id,o.status,o.type,o.estimatedDeliveryTime,"
				+ "o.finishTime,c.name,c.email,c.address from order_t o,customer_t c where o.customer_id = c.id and o.status = ?";

		List<PizzaOrder> orders = new ArrayList<>();

		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement statement = connection.prepareStatement(SQL)) {

				statement.setString(1, orderStatus.name());

				try (ResultSet rs = statement.executeQuery()) {

					while (rs.next()) {
						orders.add(buildOrder(rs));

					}
					return orders;
				}
			}
		} catch (SQLException e) {
			log.error("Error while fetching all orders", e);
			throw new RuntimeException("Error while fetching all orders", e);
		}
	}

}
