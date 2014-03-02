package org.apache.camel.impl;

/**
 * Implementation of {@link AbstractLockingRoutePolicy} for testing purposes only
 */
public class InMemoryLockingRoutePolicy extends AbstractLockingRoutePolicy {
	@Override
	protected long getExpirationTimestamp(long currentTimestamp) {
		return Long.MAX_VALUE;
	}
	@Override
	protected boolean doTryLock(RouteLock routeLock) {
		long now=getCurrentTimestamp();
		boolean  lockable=routeLock.isLockable(getRuntimeId(), now);
		log.debug("Route Lockable "+routeLock.getId()+" "+lockable);
		if (lockable) {
			log.debug("Route Lock "+routeLock.getId()+" acquired");
			routeLock.lock(getRuntimeId(), getExpirationTimestamp(now));
		}
		return lockable;
	}

	@Override
	protected void doUnlock(RouteLock routeLock) {
		boolean locked=routeLock.isLocked(getRuntimeId(), getCurrentTimestamp());
		log.debug("Route locked "+routeLock.getId()+" "+locked);
		if (locked) {
			// Released
			log.debug("Route Lock "+routeLock.getId()+" released");
			routeLock.unlock();
		}
	}

	/**
	 * For testing purpose only
	 */
	public void forceLock(String runtimeId) {
		routeLock.lock(runtimeId, Long.MAX_VALUE);
	}
	public boolean isLockOwned() {
		return routeLock.isLocked(getRuntimeId(), getCurrentTimestamp());
	}
}
