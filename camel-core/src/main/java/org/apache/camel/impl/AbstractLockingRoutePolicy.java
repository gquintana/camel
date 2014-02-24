package org.apache.camel.impl;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 */
public abstract class AbstractLockingRoutePolicy extends RoutePolicySupport implements CamelContextAware {
	/**
	 * Logger
	 */
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	/**
	 * Camel context
	 */
	private CamelContext camelContext;
	/**
	 * Id of the runtime/owner
	 */
	private String runtimeId;
	/**
	 * Executor service containing the locking loop
	 */
	private ScheduledExecutorService scheduledExecutorService;
	/**
	 * Lock polling frequency in milliseconds
	 */
	private long period = 5000L;
	/**
	 * Route state transition timeout in milliseconds
	 */
	private Long timeout = 1000L;
	/**
	 * Route info indexed by Route Id
	 */
	private Map<String, RouteInfo> routeInfosById = new HashMap<String, RouteInfo>();
	/**
	 * Route lock
	 */
	protected RouteLock routeLock=new RouteLock("global");
	/**
	 * Handle on periodically running task
	 */
	private ScheduledFuture scheduledFuture;
	/**
	 * Trick to avoid recursive calls to setRouteExpectedStatus
	 */
	private ThreadLocal<Set<String>> actualRouteStatus = new ThreadLocal<Set<String>>();

	/**
	 * Generates un pseudo unique runtime Id
	 */
	protected void initRuntimeId() {
		String hostname = null;
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
		}
		String processId = null;
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		long timestamp = runtimeMXBean.getStartTime();
		String name = runtimeMXBean.getName();
		int atPos = name.indexOf('@');
		if (atPos > 0) {
			processId = name.substring(0, atPos);
			if (hostname == null) {
				hostname = name.substring(atPos + 1);
			}
		}
		String contextName = camelContext == null ? null : camelContext.getName();
		runtimeId = "" + hostname + "-" + processId + "-" + contextName + "-" + timestamp;
	}

	/**
	 * Ran periodically to restart routes when lock is acquired
	 */
	private void checkRoutesStatus() {
		for (Route route : camelContext.getRoutes()) {
			checkRouteStatus(route);
		}
	}

	/**
	 * Try to acquire lock on given route: <ul>
	 * <li>If lock acquired, then try to start the route (or apply it's expected status)
	 * <li>If lock not acquired, then try to stop or suspend the route</li>
	 * </ul>
	 *
	 * @param route Checked route
	 */
	private void checkRouteStatus(Route route) {
		RouteInfo routeInfo = routeInfosById.get(route.getId());
		if (routeInfo == null) {
			return;
		}
		synchronized (routeInfo) {
			ServiceStatus actualStatus = simplifyRouteStatus(camelContext.getRouteStatus(route.getId()));
			ServiceStatus expectedStatus;
			if (tryLock()) {
				// Lock obtained
				// expected status is applied
				expectedStatus = routeInfo.getExpectedStatus();
				if (routeInfo.getExpectedStatus() != actualStatus) {
					try {
						if (logger.isDebugEnabled()) {
							logger.debug("Owned Route " + routeInfo.getRouteId() + ", setting status " + expectedStatus);
						}
						setActualRouteStatus(routeInfo, routeInfo.getExpectedStatus());
					} catch (Exception e) {
						handleRouteStatusException(e, route, expectedStatus);
					}
				}
			} else {
				// Lock not obtained or lost
				// At best route is suspended, else it's stopped
				expectedStatus = route.supportsSuspension() && routeInfo.getExpectedStatus() != ServiceStatus.Stopped ?
						ServiceStatus.Suspended : ServiceStatus.Stopped;
				if (expectedStatus != actualStatus) {
					if (logger.isDebugEnabled()) {
						logger.debug("Not Owned Route " + routeInfo.getRouteId() + ", setting status " + expectedStatus);
					}
					try {
						setActualRouteStatus(routeInfo, expectedStatus);
					} catch (Exception e) {
						handleRouteStatusException(e, route, expectedStatus);
					}
				}
			}
		}
	}

	/**
	 * Remove transient states
	 */
	private ServiceStatus simplifyRouteStatus(ServiceStatus routeStatus) {
		switch (routeStatus) {
			case Starting:
				routeStatus = ServiceStatus.Started;
				break;
			case Suspending:
				routeStatus = ServiceStatus.Suspended;
				break;
			case Stopping:
				routeStatus = ServiceStatus.Stopped;
		}
		return routeStatus;

	}

	/**
	 * Handle exception occured during route status change
	 *
	 * @param e              Raised exception
	 * @param route          Route
	 * @param expectedStatus Expected status
	 */
	private void handleRouteStatusException(Exception e, Route route, ServiceStatus expectedStatus) {
		if (logger.isWarnEnabled()) {
			logger.warn("Exception occured, while setting route " + route + " " + expectedStatus, e);
		}
	}

	/**
	 * Change the actual route status
	 *
	 * @param routeInfo Route info
	 * @param status    Target route status
	 * @throws Exception
	 */
	private void setActualRouteStatus(RouteInfo routeInfo, ServiceStatus status) throws Exception {
		Set<String> actualRouteIds = actualRouteStatus.get();
		if (actualRouteIds == null) {
			actualRouteIds = new HashSet<String>();
			actualRouteStatus.set(actualRouteIds);
		}
		actualRouteIds.add(routeInfo.getRouteId());
		try {
			switch (status) {
				case Stopped:
					if (timeout == null) {
						stopRoute(routeInfo.getRoute(), timeout, TimeUnit.MILLISECONDS);
					} else {
						stopRoute(routeInfo.getRoute());
					}
					break;
				case Suspended:
					if (timeout == null) {
						suspendRoute(routeInfo.getRoute());
					} else {
						suspendRoute(routeInfo.getRoute(), timeout, TimeUnit.MILLISECONDS);
					}
					break;
				case Started:
					startRoute(routeInfo.getRoute());
					break;
			}
			logger.debug("Route "+routeInfo.getRouteId()+" "+status);
		} finally {
			actualRouteIds.remove(routeInfo.getRouteId());
			if (actualRouteIds.isEmpty()) {
				actualRouteStatus.remove();
			}
		}
	}

	/**
	 * Remember the expected route status
	 *
	 * @param route  Route status
	 * @param status Expected status
	 */
	private void setExpectedRouteStatus(Route route, ServiceStatus status) {
		Set<String> actualRouteIds = actualRouteStatus.get();
		if (actualRouteStatus.get() == null || !actualRouteIds.contains(route.getId())) {
			RouteInfo routeInfo = withRouteInfo(route);
			synchronized (routeInfo) {
				routeInfo.setExpectedStatus(status);
				logger.debug("Route expected status changed to " + status);
				checkRouteStatus(route);
			}
		}
	}

	/**
	 * Try to take the lock on a route
	 *
	 * @return Lock taken or not
	 */
	protected boolean tryLock() {
		if (routeLock.isLocked(getRuntimeId(), getCurrentTimestamp())) {
			logger.debug("Route Lock "+routeLock.getId()+" already owned");
			return true;
		} else {
			synchronized (routeLock) {
				boolean locked= doTryLock(routeLock);
				logger.debug("Route Lock "+routeLock.getId()+" acquired:"+locked);
				return locked;
			}
		}
	}
	/**
	 * Try to take the lock
	 *
	 * @param routeLock Lock
	 * @return Lock taken or not
	 */
	protected abstract boolean doTryLock(RouteLock routeLock);
	/**
	 * Try to take the lock on a route
	 */
	protected void unlock() {
		if (routeLock!=null) {
			synchronized (routeLock) {
				doUnlock(routeLock);
				logger.debug("Route Lock " + routeLock.getId() + " released");
			}
		}
	}
	/**
	 * Release lock
	 *
	 * @param routeLock Lock
	 */
	protected abstract void doUnlock(RouteLock routeLock);

	// -----------------------------------------------------------------------------------------------------------------
	// ROUTE INFO MANAGEMENT

	/**
	 * Information on route
	 */
	private static class RouteInfo {
		/**
		 * Associated route
		 */
		private final Route route;
		/**
		 *
		 */
		private ServiceStatus expectedStatus;

		private RouteInfo(Route route) {
			this.route = route;
		}

		public Route getRoute() {
			return route;
		}

		public String getRouteId() {
			return route.getId();
		}

		public ServiceStatus getExpectedStatus() {
			return expectedStatus;
		}

		public void setExpectedStatus(ServiceStatus expectedStatus) {
			this.expectedStatus = expectedStatus;
		}
	}

	/**
	 * Get or create {@link RouteInfo} from a given Route
	 *
	 * @param route Route
	 * @return Associated {@link RouteInfo}
	 */
	protected RouteInfo withRouteInfo(Route route) {
		RouteInfo routeInfo = routeInfosById.get(route.getId());
		if (routeInfo == null) {
			routeInfo = new RouteInfo(route);
			routeInfosById.put(route.getId(), routeInfo);
		}
		return routeInfo;
	}

	/**
	 * Remove {@link RouteInfo} for given Route
	 *
	 * @param route Route
	 * @return Removed RouteInfo
	 */
	private RouteInfo removeRouteInfo(Route route) {
		return routeInfosById.remove(route.getId());
	}

	/**
	 * Get current timestamp
	 * @return Now timestamp
	 */
	protected long getCurrentTimestamp() {
		return System.currentTimeMillis();
	}

	/**
	 * Compute lock expiration timestamp for given current timestamp
	 * @param currentTimestamp Current timestamp
	 * @return Expiration timestamp
	 */
	protected long getExpirationTimestamp(long currentTimestamp) {
		return currentTimestamp+2*getPeriod();
	}

	// -----------------------------------------------------------------------------------------------------------------
	// ROUTE LOCK ID
	public void setRouteLockId(String lockId) {
		if (!isStopped()) {
			throw new IllegalStateException("Changing lock is allowed only when stopped");
		} else {
			routeLock = new RouteLock(lockId);
		}
	}
	public String getRouteLockId() {
		return routeLock.getId();
	}

	// -----------------------------------------------------------------------------------------------------------------
	// LIFECYCLE

	/**
	 * Start lock polling loop
	 */
	@Override
	protected void doStart() {
		if (runtimeId == null) {
			initRuntimeId();
		}
		if (scheduledExecutorService == null || scheduledExecutorService.isShutdown()) {
			scheduledExecutorService = camelContext.getExecutorServiceManager().newScheduledThreadPool(this, "LockingRoutePolicy", 3);
		}
		startRouteStatusPoller();
	}

	private void startRouteStatusPoller() {
		stopRouteStatusPoller();
		scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				logger.debug("Checking routes status");
				checkRoutesStatus();
			}
		}, 0L, period, TimeUnit.MILLISECONDS);
		logger.debug("Started route status check loop");
	}

	/**
	 * Stop lock polling loop
	 */
	@Override
	protected void doStop() throws Exception {
		unlock();
		stopRouteStatusPoller();
	}

	private void stopRouteStatusPoller() {
		if (scheduledFuture != null) {
			logger.debug("Stopping route status check loop");
			scheduledFuture.cancel(false);
			scheduledFuture = null;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	// ROUTE EVENT HANDLERS

	@Override
	public void onInit(Route route) {
		super.onInit(route);
		withRouteInfo(route);
	}

	@Override
	public void onRemove(Route route) {
		super.onRemove(route);
		removeRouteInfo(route);
	}

	@Override
	public void onStart(Route route) {
		super.onStart(route);
		setExpectedRouteStatus(route, ServiceStatus.Started);
	}

	@Override
	public void onStop(Route route) {
		super.onStop(route);
		setExpectedRouteStatus(route, ServiceStatus.Stopped);
	}

	@Override
	public void onSuspend(Route route) {
		super.onSuspend(route);
		setExpectedRouteStatus(route, ServiceStatus.Suspended);
	}

	@Override
	public void onResume(Route route) {
		super.onResume(route);
		setExpectedRouteStatus(route, ServiceStatus.Started);
	}

	// -----------------------------------------------------------------------------------------------------------------
	// GETTERS AND SETTERS

	public String getRuntimeId() {
		return runtimeId;
	}

	public void setRuntimeId(String runtimeId) {
		this.runtimeId = runtimeId;
	}

	@Override
	public CamelContext getCamelContext() {
		return camelContext;
	}

	@Override
	public void setCamelContext(CamelContext camelContext) {
		this.camelContext = camelContext;
	}

	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}

	public Long getTimeout() {
		return timeout;
	}

	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}
}
