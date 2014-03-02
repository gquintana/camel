package org.apache.camel.routepolicy.jdbc;

import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.RouteLock;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.GregorianCalendar;

/**
 * Unit test for {@link JdbcLockingRoutePolicy}
 */
public class JdbcLockingRoutePolicyTest extends CamelTestSupport {
	private JdbcLockingRoutePolicy lockingRoutePolicy = new JdbcLockingRoutePolicy();
	private EmbeddedDatabase database;
	private JdbcLockingDialect dialect = new DefaultJdbcLockingDialect();
	private long future = new GregorianCalendar(2050, 0, 1, 0, 0, 0).getTimeInMillis();

	@Before
	public void setUp() throws Exception {
		database = new EmbeddedDatabaseBuilder()
				.setType(EmbeddedDatabaseType.DERBY)
				.addScript("sql/createLockTable.sql").build();
		lockingRoutePolicy.setDataSource(database);
		lockingRoutePolicy.setPeriod(500L);
		lockingRoutePolicy.setLockTimeout(1000000L);
		lockingRoutePolicy.setStateTimeout(500L);
		lockingRoutePolicy.setRuntimeId("one");
		super.setUp();
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
		database.shutdown();
	}

	@Override
	protected RouteBuilder createRouteBuilder() throws Exception {
		return new RouteBuilder() {
			public void configure() throws Exception {
				from("direct:start").routeId("foo").routePolicy(lockingRoutePolicy)
						.to("mock:result");
			}
		};
	}

	private static interface Predicate {
		boolean matches();
	}

	private void wait(Predicate predicate, long timeout) throws InterruptedException {
		long start = System.currentTimeMillis();
		while ((System.currentTimeMillis() - start) < timeout && !predicate.matches()) {
			Thread.sleep(timeout / 10L);
		}
	}

	private void waitRouteStatus(final String routeId, final ServiceStatus routeStatus, long timeout) throws InterruptedException {
		wait(new Predicate() {
			@Override
			public boolean matches() {
				return context.getRouteStatus(routeId) == routeStatus;
			}
		}, timeout);
		assertEquals(routeStatus, context.getRouteStatus(routeId));
	}

	/**
	 * <ul>
	 * <li>Start context: lock acquired by "one"</li>
	 * </ul>
	 */
	@Test
	public void testJdbcLock_OneHasLock() throws Exception {
		assertEquals(ServiceStatus.Started, lockingRoutePolicy.getStatus());
		// Runtime one runs the route and owns the lock
		assertEquals(ServiceStatus.Started, context.getRouteStatus("foo"));
		RouteLock lock = readLockRow();
		assertEquals("one", lock.getRuntimeId());
	}

	/**
	 * <ul>
	 * <li>Start context: lock acquired by "one"</li>
	 * <li>Stop context: lock release by "one"</li>
	 * <li>Write lock file as acquired by "two"</li>
	 * <li>Start context: route remains Stopped</li>
	 * </ul>
	 */
	@Test
	public void testJdbcLock_TwoGrabsLock() throws Exception {
		// Runtime one stops and releases the lock
		stopCamelContext();
		assertEquals(ServiceStatus.Stopped, lockingRoutePolicy.getStatus());
		assertEquals(ServiceStatus.Stopped, context.getRouteStatus("foo"));
		// Runtime two runs the route
		writeLockRow("two", future);
		waitRow("two", 1000L);
		startCamelContext();
		assertEquals(ServiceStatus.Started, lockingRoutePolicy.getStatus());
		assertEquals("two", readLockRow().getRuntimeId());
		// Runtime one has route stopped
		waitRouteStatus("foo", ServiceStatus.Suspended, 2000L);
	}

	/**
	 * <ul>
	 * <li>Start context: lock acquired by "one"</li>
	 * <li>Stop context: lock release by "one"</li>
	 * <li>Write lock file as acquired by "two"</li>
	 * <li>Start context: route remains Stopped</li>
	 * <li>Write lock file as released by "two"</li>
	 * <li>Route is restarted</li>
	 * </ul>
	 */
	@Test
	public void testJdbcLock_TwoReleasesLock() throws Exception {
		// Runtime one stops and releases the lock
		stopCamelContext();
		waitRouteStatus("foo", ServiceStatus.Stopped, 1000L);
		// Runtime two runs the route and Runtime one has route stopped
		writeLockRow("two", future);
		waitRow("two", 1000L);
		log.debug("Lock acquired by two");
		context.start();
		assertEquals("two", readLockRow().getRuntimeId());
		waitRouteStatus("foo", ServiceStatus.Suspended, 1000L);
		log.debug("Route suspended");
		// Runtime two releases lock
		writeLockRow(null, 0);
		log.debug("Lock released by two");
		// Runtime one restarts route
		waitRouteStatus("foo", ServiceStatus.Started, 1000L);
		log.debug("Route started");
		waitRow("one", 1000L);
	}

	private void waitRow(final String runtimeId, long timeout) throws InterruptedException {
		wait(new Predicate() {
			@Override
			public boolean matches() {
				try {
					RouteLock lock = readLockRow();
					return lock != null && lock.getRuntimeId().equals(runtimeId);
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}, timeout);
	}

	private RouteLock readLockRow() throws SQLException {
		Connection connection = database.getConnection();
		try {
			return dialect.select(connection, "global", false, 1000L);
		} finally {
			connection.close();
		}
	}

	private void writeLockRow(String runtimeId, long expiration) throws SQLException {
		Connection connection = database.getConnection();
		try {
			dialect.update(connection, "global", runtimeId, expiration, 1000L);
			connection.commit();
		} finally {
			connection.close();
		}
	}

}
