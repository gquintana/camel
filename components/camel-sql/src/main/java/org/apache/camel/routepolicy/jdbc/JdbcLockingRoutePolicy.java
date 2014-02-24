package org.apache.camel.routepolicy.jdbc;

import org.apache.camel.impl.AbstractLockingRoutePolicy;
import org.apache.camel.impl.RouteLock;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/**
 */
public class JdbcLockingRoutePolicy extends AbstractLockingRoutePolicy {
	private PlatformTransactionManager transactionManager;
	private DataSource dataSource;
	private JdbcTemplate jdbcTemplate;
	private TransactionTemplate transactionTemplate;
	private String tableName="CAMEL_LOCK";
	private RowMapper<RouteLock> routeLockRowMapper=new RouteLockRowMapper();

	private static class RouteLockRowMapper implements RowMapper<RouteLock> {
		private <T> T handleNull(T value, ResultSet resultSet) throws SQLException {
			return value==null || resultSet.wasNull() ? null : value;
		}
		@Override
		public RouteLock mapRow(ResultSet resultSet, int i) throws SQLException {
			String id = resultSet.getString(1);
			String runtimeId= handleNull(resultSet.getString(2), resultSet);
			java.sql.Timestamp timestamp=handleNull(resultSet.getTimestamp(3), resultSet);
			return new RouteLock(id, runtimeId, timestamp.getTime());
		}
	};
	private RouteLock select(String lockId, boolean forUpdate) {
		return jdbcTemplate.queryForObject("select ID,RUNTIME_ID,EXPIRATION_DATE from "+tableName+" where ID=?"+ (forUpdate?" for update":""), new Object[]{lockId}, routeLockRowMapper);
	}
	private void insert(String lockId) {
		jdbcTemplate.update("insert into " + tableName + "(ID,RUNTIME_ID,EXPIRATION_DATE) values (?,?,?)", new Object[]{lockId, null, null}, new int[]{Types.VARCHAR,Types.VARCHAR,Types.TIMESTAMP});
	}
	private int update(String lockId, String runtimeId, long timestamp) {
		return jdbcTemplate.update("update " + tableName + " set RUNTIME_ID=?,EXPIRATION_DATE=? where ID=?", new Object[]{runtimeId, new Timestamp(timestamp), lockId}, new int[]{Types.VARCHAR,Types.TIMESTAMP,Types.VARCHAR});
	}
	private void reset(String lockId, String runtime) {
		jdbcTemplate.update("update " + tableName + " set EXPIRATION_DATE=? where ID=? and RUNTIME_ID=?", new Object[]{null, lockId, runtime}, new int[]{Types.TIMESTAMP,Types.VARCHAR,Types.VARCHAR});
	}

	@Override
	protected boolean doTryLock(final RouteLock routeLock) {
		try {
			return transactionTemplate.execute(new TransactionCallback<Boolean>() {
				@Override
				public Boolean doInTransaction(TransactionStatus status) {
					return tryLockInTx(routeLock);
				}
			});
		} catch (ConcurrencyFailureException e) {
			return false;
		} catch (DuplicateKeyException e) {
			return false;
		}
	}

	private Boolean tryLockInTx(RouteLock lock) {
		// Synchronize/read lock with database
		try {
			RouteLock readLock = select(lock.getId(), false);
			lock.lock(readLock.getRuntimeId(),readLock.getExpirationTimestamp());
		} catch (EmptyResultDataAccessException lockNotExistExc) {
			// Retry
			insert(lock.getId()); // May throw DuplicateKeyException
		}
		// Try to lock
		select(lock.getId(), true); // May throw ConcurrencyFailureException
		long now=getCurrentTimestamp();
		boolean locked = lock.isLockable(getRuntimeId(), now);
		if (locked) {
			long expiration=getExpirationTimestamp(now);
			// Synchronize/write lock with database
			if (update(lock.getId(), getRuntimeId(), expiration)==1) { // May throw ConcurrencyFailureException
				lock.lock(getRuntimeId(), expiration);
			}
		}
		return locked;
	}

	@Override
	protected void doUnlock(final RouteLock lock) {
		transactionTemplate.execute(new TransactionCallback<Void>() {
			@Override
			public Void doInTransaction(TransactionStatus status) {
				unlockInTx(lock);
				return null;
			}
		});
		
	}

	private void unlockInTx(RouteLock lock) {
		reset(lock.getId(), getRuntimeId());
	}

	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
		this.transactionTemplate = new TransactionTemplate(transactionManager);
		if (this.getTimeout()!=null) {
			this.transactionTemplate.setTimeout(this.getTimeout().intValue());
		}
		this.transactionTemplate.setTimeout(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		if (this.getTimeout()!=null) {
			this.jdbcTemplate.setQueryTimeout(this.getTimeout().intValue());
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
