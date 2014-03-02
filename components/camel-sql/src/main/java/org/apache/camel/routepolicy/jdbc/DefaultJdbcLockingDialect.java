package org.apache.camel.routepolicy.jdbc;

import org.apache.camel.impl.RouteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Default implemntation of {@link JdbcLockingDialect} using standard SQL
 */
public class DefaultJdbcLockingDialect implements JdbcLockingDialect {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	/**
	 * Name of table used to store locks.
	 * Defaults to CAMEL_LOCK
	 */
	private String tableName = "CAMEL_LOCK";

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RouteLock select(Connection connection, String lockId, boolean forUpdate, long timeout) throws SQLException {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			preparedStatement = connection.prepareStatement("select ID, RUNTIME_ID, EXPIRATION_DATE from " + tableName + " where ID=?" + (forUpdate ? " for update" : ""));
			preparedStatement.setString(1, lockId);
			setQueryTimeout(preparedStatement, timeout);
			resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				String runtimeId = resultSet.getString(2);
				if (resultSet.wasNull()) {
					runtimeId = null;
				}
				Timestamp timestamp = resultSet.getTimestamp(3);
				if (resultSet.wasNull()) {
					timestamp = null;
				}
				return new RouteLock(lockId, runtimeId, timestamp == null ? null : timestamp.getTime());
			} else {
				return null;
			}
		} finally {
			closeSilently(preparedStatement, resultSet);
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public int insert(Connection connection, String lockId, long timeout) throws SQLException {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement("insert into " + tableName + "(ID) values (?)");
			preparedStatement.setString(1, lockId);
			setQueryTimeout(preparedStatement, timeout);
			return preparedStatement.executeUpdate();
		} finally {
			closeSilently(preparedStatement, null);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int update(Connection connection, String lockId, String runtimeId, Long expiration, long timeout) throws SQLException {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement("update " + tableName + " set RUNTIME_ID=?, EXPIRATION_DATE=? where ID=?");
			if (runtimeId == null) {
				preparedStatement.setNull(1, Types.VARCHAR);
			} else {
				preparedStatement.setString(1, runtimeId);
			}
			if (expiration == null) {
				preparedStatement.setNull(2, Types.TIMESTAMP);
			} else {
				preparedStatement.setTimestamp(2, new java.sql.Timestamp(expiration));
			}
			preparedStatement.setString(3, lockId);
			setQueryTimeout(preparedStatement, timeout);
			return preparedStatement.executeUpdate();
		} finally {
			closeSilently(preparedStatement, null);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int reset(Connection connection, String lockId, String runtimeId, long timeout) throws SQLException {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement("update " + tableName + " set EXPIRATION_DATE=null, RUNTIME_ID=null where ID=? and RUNTIME_ID=?");
			preparedStatement.setString(1, lockId);
			preparedStatement.setString(2, runtimeId);
			setQueryTimeout(preparedStatement, timeout);
			return preparedStatement.executeUpdate();
		} finally {
			closeSilently(preparedStatement, null);
		}
	}

	/**
	 * Apply timeout on {@link PreparedStatement} or throw {@link SQLTimeoutException} if already expired
	 */
	private void setQueryTimeout(PreparedStatement preparedStatement, long timeout) throws SQLException {
		if (timeout>0) {
			preparedStatement.setQueryTimeout((int) timeout);
		} else {
			throw new SQLTimeoutException();
		}
	}

	/**
	 * Release {@link ResultSet} and {@link PreparedStatement}
	 */
	private void closeSilently(PreparedStatement statement, ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
