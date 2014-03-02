package org.apache.camel.routepolicy.jdbc;

import org.apache.camel.impl.RouteLock;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface of SQL dialect used for storing lock in database
 */
public interface JdbcLockingDialect {
	/**
	 * Read lock from database
	 * @param connection Database connection
	 * @param lockId Lock Id
	 * @param forUpdate Enable pessimistic locking
	 * @return Read lock or null if not found
	 * @throws SQLException
	 */
	RouteLock select(Connection connection, String lockId, boolean forUpdate, long timeout) throws SQLException;

	/**
	 * Insert lock in database
	 * @param connection Database connection
	 * @param lockId Lock Id
	 * @return 1 if insert, 0 either
	 * @throws SQLException
	 */
	int insert(Connection connection, String lockId, long timeout) throws SQLException;

	/**
	 * Release lock if owned in database
	 * @param connection Database connection
	 * @param lockId Lock Id
	 * @return 1 if updated, 0 either
	 * @throws SQLException
	 */
	int reset(Connection connection, String lockId, String runtimeId, long timeout) throws SQLException;

	/**
	 * Acquire lock
	 * @param connection Database connection
	 * @param lockId Lock Id
	 * @param runtimeId Runtime Id
	 * @param expiration Expiration timestamp
	 * @return 1 if updated, 0 either
	 * @throws SQLException
	 */
	int update(Connection connection, String lockId, String runtimeId, Long expiration, long timeout) throws SQLException;
}
