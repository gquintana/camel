package org.apache.camel.routepolicy.jdbc;

import org.apache.camel.impl.AbstractLockingRoutePolicy;
import org.apache.camel.impl.RouteLock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link AbstractLockingRoutePolicy} using a shared database
 * to determine which node runs the routes.
 */
public class JdbcLockingRoutePolicy extends AbstractLockingRoutePolicy {
	/**
	 * JDBC DataSource
	 */
	private DataSource dataSource;
	/**
	 * JDBC Dialect contains SQL query, can be overriden for specific database
	 */
	private JdbcLockingDialect dialect = new DefaultJdbcLockingDialect();

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doTryLock(final RouteLock routeLock) {
		Connection connection = null;
		try {
			connection = getConnection();
			boolean result = tryLockInTx(connection, routeLock);
			connection.commit();
			return result;
		} catch (SQLException e) {
			log.error("e");
			// TODO Handle SQLException
			throw new IllegalStateException(e);
		} finally {
			closeSilently(connection);
		}
	}

	/**
	 * Try to acquire lock on database
	 * @param connection Database connection
	 * @param lock Lock
	 * @return true if lock acquired
	 * @throws SQLException
	 */
	private boolean tryLockInTx(Connection connection, RouteLock lock) throws SQLException {
		final long endTimestamp=getCurrentTimestamp()+getLockTimeout();
		// Synchronize/read lock with database
		RouteLock readLock = dialect.select(connection, lock.getId(), false, getLockTimeout());
		if (readLock == null) {
			dialect.insert(connection, lock.getId(), endTimestamp-getCurrentTimestamp()); // May throw duplicate key
		} else {
			lock.lock(readLock.getRuntimeId(), readLock.getExpirationTimestamp());
		}
		log.debug("Read lock "+lock.getId()+" "+lock.getRuntimeId()+" "+lock.getExpirationTimestamp());
		// Try to lock
		dialect.select(connection, lock.getId(), true, endTimestamp-getCurrentTimestamp()); // May throw concurrency failure
		long now = getCurrentTimestamp();
		boolean locked = lock.isLockable(getRuntimeId(), now);
		if (locked) {
			long expiration = getExpirationTimestamp(now);
			// Synchronize/write lock with database
			if (dialect.update(connection, lock.getId(), getRuntimeId(), expiration, endTimestamp-getCurrentTimestamp()) == 1) { // May throw concurrency failure
				lock.lock(getRuntimeId(), expiration);
			}
		}
		return locked;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void doUnlock(final RouteLock routeLock) {
		Connection connection = null;
		try {
			connection = getConnection();
			unlockInTx(connection, routeLock);
			connection.commit();
		} catch (SQLException e) {
			// TODO Handle Exception
			throw new IllegalStateException(e);
		} finally {
			closeSilently(connection);
		}
	}

	/**
	 * Release lock if lock is owned
	 * @param connection Database connection
	 * @param lock Lock
	 * @throws SQLException
	 */
	private void unlockInTx(Connection connection, RouteLock lock) throws SQLException {
		dialect.reset(connection, lock.getId(), getRuntimeId(), getLockTimeout());
		if (lock.isLocked(getRuntimeId(), getCurrentTimestamp())) {
			lock.unlock();
		}
	}

	/**
	 * Get connection from datasource
	 * @return Connection
	 * @throws SQLException
	 */
	private Connection getConnection() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}

	private void closeSilently(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
}
