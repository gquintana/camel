package org.apache.camel.component.file.routepolicy;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.util.FileUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link FileLockingRoutePolicy}
 */
public class FileLockingRoutePolicyTest extends ContextTestSupport {
	public File tempFolder;
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private FileLockingRoutePolicy lockingRoutePolicy = new FileLockingRoutePolicy();
	@Override
	protected RouteBuilder createRouteBuilder() throws Exception {
		File file=File.createTempFile("FileLockingRoute","Test");
		file.delete();
		file.mkdir();
		tempFolder=file;
		lockingRoutePolicy.setPeriod(500L);
		lockingRoutePolicy.setFolder(tempFolder);
		lockingRoutePolicy.setRuntimeId("one");
		return new RouteBuilder() {
			public void configure() throws Exception {
				from("direct:start").routeId("foo").routePolicy(lockingRoutePolicy)
						.to("mock:result");
			}
		};
	}
	@Override
	protected void tearDown() throws Exception {
		FileUtil.removeDir(tempFolder);
		super.tearDown();
	}
	private File getLockFile() {
		return new File(tempFolder,"global.lck");
	}

	/**
	 * Read lock file content
	 */
	public String readFile(File file) throws IOException {
		BufferedReader fileReader=null;
		try {
		fileReader=new BufferedReader(new FileReader(file));
		return fileReader.readLine();
		}finally {
			closeSilently(fileReader);
		}
	}

	/**
	 * Write lock file content
	 */
	public void writeFile(File file, String content) throws IOException {
		BufferedWriter fileWriter=null;
		try {
			fileWriter=new BufferedWriter(new FileWriter(file));
			fileWriter.write(content);
			fileWriter.flush();
			fileWriter.close();
			logger.debug("Wrote lock file " + content);
		} finally {
			closeSilently(fileWriter);
		}
	}

	private void closeSilently(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	private static interface Predicate {
		boolean matches();
	}
	private void wait(Predicate predicate, long timeout) throws InterruptedException {
		long start=System.currentTimeMillis();
		while ((System.currentTimeMillis()-start)<timeout && !predicate.matches()) {
			Thread.sleep(timeout/10L);
		}
	}
	private void waitRouteStatus(final String routeId, final ServiceStatus routeStatus, long timeout) throws InterruptedException {
		wait(new Predicate() {
			@Override
			public boolean matches() {
				return context.getRouteStatus(routeId)==routeStatus;
			}
		}, timeout);
		assertEquals(routeStatus, context.getRouteStatus(routeId));
	}
	private void waitLockFileContent(final String prefix, long timeout) throws InterruptedException, IOException {
		wait(new Predicate() {
			@Override
			public boolean matches() {
				try {
					return readFile(getLockFile()).startsWith(prefix);
				} catch (IOException e) {
					return false;
				}
			}
		}, timeout);
		assertThat(readFile(getLockFile()), startsWith(prefix));
	}
	/**
	 * <ul>
	 *     <li>Start context: lock acquired by "one"</li>
	 * </ul>
	 */
	@Test
	public void testFileLock_OneHasLock() throws Exception {
		File lockFile= getLockFile();
		assertEquals(ServiceStatus.Started, lockingRoutePolicy.getStatus());
		// Runtime one runs the route and owns the lock
		assertEquals(ServiceStatus.Started, context.getRouteStatus("foo"));
		String content=readFile(lockFile).trim();
		assertTrue(Pattern.compile("global\\tone\\t\\d+").matcher(content).matches());
	}

	/**
	 * <ul>
	 *     <li>Start context: lock acquired by "one"</li>
	 *     <li>Stop context: lock release by "one"</li>
	 *     <li>Write lock file as acquired by "two"</li>
	 *     <li>Start context: route remains Stopped</li>
	 * </ul>
	 */
	@Test
	public void testFileLock_TwoGrabsLock() throws Exception {
		File lockFile= getLockFile();
		// Runtime one stops and releases the lock
		stopCamelContext();
		assertEquals(ServiceStatus.Stopped, lockingRoutePolicy.getStatus());
		assertEquals(ServiceStatus.Stopped, context.getRouteStatus("foo"));
		// Runtime two runs the route
		freeMappedLockFile();
		writeFile(lockFile, "global\ttwo\t" + Long.MAX_VALUE);
		waitLockFileContent("global\ttwo\t", 1000L);
		freeMappedLockFile();
		startCamelContext();
		assertEquals(ServiceStatus.Started, lockingRoutePolicy.getStatus());
		assertThat(readFile(lockFile), startsWith("global\ttwo"));
		// Runtime one has route stopped
		waitRouteStatus("foo", ServiceStatus.Suspended, 2000L);
	}

	/**
	 * Mapped file are freed by GC
	 */
	private void freeMappedLockFile() {
		System.gc();
	}

	/**
	 * <ul>
	 *     <li>Start context: lock acquired by "one"</li>
	 *     <li>Stop context: lock release by "one"</li>
	 *     <li>Write lock file as acquired by "two"</li>
	 *     <li>Start context: route remains Stopped</li>
	 *     <li>Write lock file as released by "two"</li>
	 *     <li>Route is restarted</li>
	 * </ul>
	 */
	@Test
	public void testFileLock_TwoReleasesLock() throws Exception {
		File lockFile= getLockFile();
		// Runtime one stops and releases the lock
		stopCamelContext();
		// Runtime two runs the route and Runtime one has route stopped
		freeMappedLockFile();
		writeFile(lockFile, "global\ttwo\t" + Long.MAX_VALUE);
		waitLockFileContent("global\ttwo\t", 1000L);
		logger.debug("Lock acquired by two");
		context.start();
		assertThat(readFile(lockFile), startsWith("global\ttwo"));
		waitRouteStatus("foo", ServiceStatus.Suspended, 1000L);
		logger.debug("Route suspended");
		// Runtime two releases lock
		freeMappedLockFile();
		writeFile(lockFile, "");
		logger.debug("Lock released by two");
		// Runtime one restarts route
		waitRouteStatus("foo", ServiceStatus.Started, 1000L);
		logger.debug("Route started");
		waitLockFileContent("global\tone\t", 1000L);
	}

}
