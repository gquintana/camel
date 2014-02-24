package org.apache.camel.impl;

/**
 * Lock for one or many routes
 */
public class RouteLock {
	/**
	 * Lock Id
	 */
	private final String id;
	/**
	 * Runtime Id (who is owning the lock)
	 */
	private String runtimeId;
	/**
	 * Expiration date (until when the lock is valid)
	 */
	private Long expirationTimestamp;
	/**
	 * Extra info used by locking route policies
	 */
	private Object data;

	public RouteLock(String id) {
		this.id = id;
	}

	public RouteLock(String id, String runtimeId, Long expirationTimestamp) {
		this.id = id;
		this.runtimeId = runtimeId;
		this.expirationTimestamp = expirationTimestamp;
	}

	public String getId() {
		return id;
	}

	public String getRuntimeId() {
		return runtimeId;
	}

	public Long getExpirationTimestamp() {
		return expirationTimestamp;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	/**
	 * Indicate if this lock can be locked: it's not locked by someone else.
	 * Either because it's not locked, or it's locked by current runtime, or it has expired
	 * @param currentRuntimeId Current runtime Id
	 * @param currentTimestamp Current timestamp
	 * @return Lockable
	 */
	public boolean isLockable(String currentRuntimeId, long currentTimestamp) {
		return !(this.runtimeId != null
				&& !this.runtimeId.equals(currentRuntimeId)
				&& this.expirationTimestamp != null
				&& currentTimestamp <= expirationTimestamp);
	}
	/**
	 * Indicate if this lock is locked by given runtime
	 * Either because it's not locked, or it's locked by current runtime, or it has expired
	 * @param currentRuntimeId Current runtime Id
	 * @param currentTimestamp Current timestamp
	 * @return Lockable
	 */
	public boolean isLocked(String currentRuntimeId, long currentTimestamp) {
		return this.runtimeId != null
				&& this.runtimeId.equals(currentRuntimeId)
				&& this.expirationTimestamp != null
				&& currentTimestamp <= expirationTimestamp;
	}
	public void lock(String runtimeId, Long expirationTimestamp) {
		this.runtimeId=runtimeId;
		this.expirationTimestamp=expirationTimestamp;
	}
	public void unlock() {
		this.runtimeId=null;
		this.expirationTimestamp=null;
	}
}
