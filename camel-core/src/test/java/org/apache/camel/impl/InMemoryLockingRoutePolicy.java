package org.apache.camel.impl;

import org.apache.camel.Route;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link AbstractLockingRoutePolicy} for testing purposes only
 */
public class InMemoryLockingRoutePolicy extends AbstractLockingRoutePolicy {
	private final Map<String, RouteLock> locks=new HashMap<String, RouteLock>();
	public static class RouteLock {
		private String ownerId;
		public String getOwnerId() {
			return ownerId;
		}

		public synchronized void setOwnerId(String ownerId) {
			this.ownerId = ownerId;
		}

		public synchronized boolean isOwnedBy(String runtimeId) {
			return this.ownerId!=null && this.ownerId.equals(runtimeId);
		}
	}
	public RouteLock withLock(String id) {
		RouteLock lock=locks.get(id);
		if (lock==null) {
			lock=new RouteLock();
			locks.put(id, lock);
		}
		return lock;
	}
	private RouteLock withLock(Route route) {
		return withLock(route.getId());
	}

	@Override
	protected boolean tryLock(Route route) {
		RouteLock lock= withLock(route);
		boolean acquired;
		if (lock.getOwnerId()==null) {
			// Not owned at all
			lock.setOwnerId(getRuntimeId());
			acquired=true;
		} else if (lock.isOwnedBy(getRuntimeId())) {
			// Already owned by same runtime
			acquired=true;
		} else {
			// Already owned by another runtime
			acquired=false;
		}
		return acquired;
	}

	@Override
	protected void unlock(Route route) {
		RouteLock lock= withLock(route);
		if (lock.isOwnedBy(getRuntimeId())) {
			// Released
			lock.setOwnerId(null);
		}
	}

	/**
	 * For testing purpose only
	 */
	public void forceLock(String lockId, String ownerId) {
		withLock(lockId).setOwnerId(ownerId);
	}
	public boolean isLockOwned(String lockId) {
		return withLock(lockId).isOwnedBy(getRuntimeId());
	}
}
