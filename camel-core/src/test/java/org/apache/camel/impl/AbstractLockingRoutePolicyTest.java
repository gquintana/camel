package org.apache.camel.impl;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 */
public class AbstractLockingRoutePolicyTest extends ContextTestSupport {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private InMemoryLockingRoutePolicy lockingRoutePolicy = new InMemoryLockingRoutePolicy();

	@Override
	protected RouteBuilder createRouteBuilder() throws Exception {
		lockingRoutePolicy.setPeriod(500L);
		return new RouteBuilder() {
			public void configure() throws Exception {
				from("direct:parent").routeId("parent")
						.to("direct:start");
				from("direct:start").routeId("foo").routePolicy(lockingRoutePolicy)
						.to("direct:child");
				from("direct:child").routeId("child")
						.to("mock:result");
			}
		};
	}

	@Test
	public void testInitRuntimeId() {
		lockingRoutePolicy.initRuntimeId();
		String runtimeId = lockingRoutePolicy.getRuntimeId();
		assertTrue(Pattern.compile(".*-\\d+-.*-\\d+").matcher(runtimeId).matches());
	}
	private void waitRouteStatus(String routeId, ServiceStatus expectedStatus, long timeout) throws InterruptedException {
		long start=System.currentTimeMillis();
		ServiceStatus actualStatus;
		logger.debug("Waiting for route "+routeId+" to be state "+expectedStatus);
		while((actualStatus=context.getRouteStatus(routeId))!=expectedStatus && (System.currentTimeMillis()-start)<timeout) {
			Thread.sleep(100L);
		}
		if (actualStatus !=expectedStatus) {
			fail("Route "+routeId+" stated is not "+expectedStatus+", but "+ actualStatus);
		}
	}

	/**
	 * Test route status change when lock is owned
	 */
	@Test
	public void testOwned_StatusChange() throws Exception {
		// Auto start
		assertEquals(ServiceStatus.Started, context.getRouteStatus("foo"));
		assertTrue(lockingRoutePolicy.isLockOwned("foo"));
		// Suspend
		context.suspendRoute("foo");
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
		// Resume
		context.resumeRoute("foo");
		assertEquals(ServiceStatus.Started, context.getRouteStatus("foo"));
		// Stop
		context.stopRoute("foo");
		assertEquals(ServiceStatus.Stopped, context.getRouteStatus("foo"));
		// Start
		context.startRoute("foo");
		assertEquals(ServiceStatus.Started, context.getRouteStatus("foo"));
	}
	/**
	 * Test route status change when lock is not owned
	 */
	@Test
	public void testNotOwned_StatusChange() throws Exception {
		// Another runtime gets the lock
		lockingRoutePolicy.forceLock("foo", "another");
		// Auto start
		waitRouteStatus("foo",ServiceStatus.Suspended, 1000L);
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
		assertFalse(lockingRoutePolicy.isLockOwned("foo"));
		// Suspend
		context.suspendRoute("foo");
		waitRouteStatus("foo",ServiceStatus.Suspended, 200L);
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
		// Resume
		context.resumeRoute("foo");
		waitRouteStatus("foo",ServiceStatus.Suspended, 1000L);
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
		// Stop
		context.stopRoute("foo");
		waitRouteStatus("foo",ServiceStatus.Stopped, 200L);
		assertEquals(ServiceStatus.Stopped, context.getRouteStatus("foo"));
		// Start
		context.startRoute("foo");
		// TODO During a short period of time route is started... then suspended
		waitRouteStatus("foo",ServiceStatus.Suspended, 1000L);
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
	}
	/**
	 * Test lock acquisition and release
	 */
	@Test
	public void testRelease() throws Exception {
		// Another runtime gets the lock, route suspends
		lockingRoutePolicy.forceLock("foo", "another");
		waitRouteStatus("foo",ServiceStatus.Suspended, 1000L);
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
		assertFalse(lockingRoutePolicy.isLockOwned("foo"));

		// Other runtime releases the lock, route starts
		lockingRoutePolicy.forceLock("foo", null);
		waitRouteStatus("foo",ServiceStatus.Started, 1000L);
		assertEquals(ServiceStatus.Started, context.getRouteStatus("foo"));

		// Other runtime takes the lock again, route suspends
		lockingRoutePolicy.forceLock("foo", "another");
		waitRouteStatus("foo",ServiceStatus.Suspended, 1000L);
		assertEquals(ServiceStatus.Suspended, context.getRouteStatus("foo"));
	}
}
