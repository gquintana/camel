package org.apache.camel.impl;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link RouteLock}
 */
public class RouteLockTest {
	private RouteLock lock=new RouteLock("id");
	private long now=System.currentTimeMillis();
	@Test
	public void testIsLockable() throws Exception {
		// Not locked
		assertTrue(lock.isLockable("foo", now - 10L));
		assertTrue(lock.isLockable("foo", now + 10L));
		// Locked by foo until now
		lock.lock("foo", now);
		assertTrue(lock.isLockable("foo", now - 10L));
		assertTrue(lock.isLockable("foo", now + 10L));
		// Locked by bar until now
		lock.lock("bar", now);
		assertFalse(lock.isLockable("foo", now - 10L));
		assertTrue(lock.isLockable("foo", now + 10L));
	}

	@Test
	public void testIsLocked() throws Exception {
		// Not locked
		assertFalse(lock.isLocked("foo", now - 10L));
		assertFalse(lock.isLocked("foo", now + 10L));
		// Locked by foo until now
		lock.lock("foo", now);
		assertTrue(lock.isLocked("foo", now - 10L));
		assertFalse(lock.isLocked("foo", now + 10L));
		// Locked by bar until now
		lock.lock("bar", now);
		assertFalse(lock.isLocked("foo", now - 10L));
		assertFalse(lock.isLocked("foo", now + 10L));
	}
}
