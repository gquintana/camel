package org.apache.camel.component.file.routepolicy;

import org.apache.camel.impl.AbstractLockingRoutePolicy;
import org.apache.camel.impl.RouteLock;
import org.apache.camel.util.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;

/**
 * Locking route policy using a shared file to tell which runtime
 * is owning the lock.
 */
public class FileLockingRoutePolicy extends AbstractLockingRoutePolicy {
	/**
	 * Folder where locks will be created
	 */
	private File folder;
	/**
	 * Charset used to write in file
	 */
	private Charset charset = Charset.defaultCharset();
	/**
	 * Buffer size for read/write in file
	 */
	private int fileBufferSize = 1024;
	/**
	 * Use file locking
	 */
	private boolean lockFile=false;
	/**
	 * Convert lock id into File
	 *
	 * @param lockId Lock Id
	 * @return File
	 */
	protected File getLockFile(String lockId) {
		return new File(folder, lockId.replaceAll("[^\\w-]+", "_") + ".lck");
	}

	private static class LockFileData {
		private final RandomAccessFile randomAccessFile;
		private final FileChannel fileChannel;
		private final FileLock fileLock;
		private final MappedByteBuffer byteBuffer;
		private LockFileData(RandomAccessFile randomAccessFile, FileChannel fileChannel, FileLock fileLock, MappedByteBuffer byteBuffer) {
			this.randomAccessFile = randomAccessFile;
			this.fileChannel = fileChannel;
			this.fileLock = fileLock;
			this.byteBuffer = byteBuffer;
		}

		public FileChannel getFileChannel() {
			return fileChannel;
		}

		public FileLock getFileLock() {
			return fileLock;
		}

		public MappedByteBuffer getByteBuffer() {
			return byteBuffer;
		}

		public RandomAccessFile getRandomAccessFile() {
			return randomAccessFile;
		}
	}

	private void read(ByteBuffer byteBuffer, RouteLock lock) throws IOException {
		byteBuffer.rewind();
		String content = charset.decode(byteBuffer).toString();
		if (content!=null) {
			content = content.trim();
		}
		if (content==null || content.isEmpty()) {
			lock.unlock();
			log.debug("Read lock file: empty");
			return;
		}
		log.debug("Read lock file:"+content);
		String[] parts = content.split("\\t");
		if (parts.length == 3) {
			String lockId = parts[0];
			if (lockId!=null && !lockId.equals(lock.getId())) {
				throw new IOException("Invalid Lock Id");
			}
			String runtimeId = parts[1];
			if (runtimeId == null || runtimeId.isEmpty()) {
				runtimeId = null;
			}
			Long expirationTimestamp;
			if (parts[2] == null || parts[2].isEmpty()) {
				expirationTimestamp = null;
			} else {
				expirationTimestamp = Long.valueOf(parts[2]);
			}
			lock.lock(runtimeId, expirationTimestamp);
		} else {
			throw new IOException("Invalid lock file format");
		}
	}

	private int write(ByteBuffer byteBuffer, RouteLock lock) {
		byteBuffer.clear();
		StringBuilder contentBuilder = new StringBuilder(lock.getId())
				.append("\t");
		if (lock.getRuntimeId() != null) {
			contentBuilder.append(lock.getRuntimeId());
		}
		contentBuilder.append("\t");
		if (lock.getExpirationTimestamp() != null) {
			contentBuilder.append(lock.getExpirationTimestamp().toString());
		}
		String content = contentBuilder.toString();
		ByteBuffer byteBuffer1 = charset.encode(content);
		byteBuffer.put(byteBuffer1);
		byteBuffer.limit(byteBuffer1.limit());
		log.debug("Wrote lock file:" + content);
		return byteBuffer1.limit();
	}


	@Override
	protected boolean doTryLock(RouteLock lock) {
		boolean routeLocked=false;
		LockFileData lockFileData = (LockFileData) lock.getData(),
			finalLockFileData = null; // By default lock file data will be cleared

		FileChannel fileChannel = null;
		FileLock fileLock = null;
		MappedByteBuffer byteBuffer = null;
		RandomAccessFile randomAccessFile = null;
		try {
			if (lockFileData != null) {
				// Check File lock
				fileLock = lockFileData.getFileLock();
				if (fileLock != null && !fileLock.isValid()) {
					releaseFileLockSilently(fileLock);
					fileLock = null;
				}
				// Check File channel
				fileChannel = lockFileData.getFileChannel();
				byteBuffer = lockFileData.getByteBuffer();
				if (fileChannel != null && !fileChannel.isOpen()) {
					closeSilently(fileChannel);
					fileChannel = null;
					byteBuffer = null;
				}
				randomAccessFile = lockFileData.getRandomAccessFile();
			}
			// Random Access file
			boolean readOnly=false;
			if (randomAccessFile == null) {
				File lockFile = getLockFile(lock.getId());
				log.debug("Lock file "+lockFile);
				// Create file
				if (!lockFile.exists()) {
					FileUtil.createNewFile(lockFile); // Won't throw file already exists
				}
				// Open file
				try {
					randomAccessFile = new RandomAccessFile(lockFile, "rwd"); // May throw File not found
				} catch (FileNotFoundException e) {
					randomAccessFile = new RandomAccessFile(lockFile, "r"); // May throw File not found
					readOnly=true;
				}
			}
			// Channel
			if (fileChannel == null) {
				fileChannel=randomAccessFile.getChannel();
			}
			// Lock file if need (some FS don't support file locking)
			boolean fileLocked;
			if (fileLock!=null) { // Already locked
				fileLocked=true;
			} else if (lockFile) { // File lock required
				fileLock = fileChannel.tryLock();
				fileLocked = fileLock != null;
			} else { // No file lock needed
				fileLocked = true;
			}
			// Map file
			if (byteBuffer==null) {
				if (readOnly) {
					log.debug("Opening lock file RO");
					byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileBufferSize);
				} else {
					log.debug("Opening lock file R/W");
					byteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileBufferSize);
				}
			}
			// Read file
			read(byteBuffer, lock);
			if (!readOnly && fileLocked) {
				// Update lock
				long now = getCurrentTimestamp();
				boolean locked = lock.isLockable(getRuntimeId(), now);
				if (locked) {
					// Write file
					lock.lock(getRuntimeId(), now);
					int size=write(byteBuffer, lock);
					routeLocked=true;
					finalLockFileData= lockFileData==null ? new LockFileData(randomAccessFile, fileChannel, fileLock, byteBuffer) : lockFileData;
				} else {
					// Lock lost
					closeAllSilently(fileLock, fileChannel, randomAccessFile);
				}
			}
		} catch (IOException e) {
			log.warn("File lock read/write failed", e);
			closeAllSilently(fileLock, fileChannel, randomAccessFile);
		} finally {
			log.debug("Set Lock file data "+finalLockFileData);
			lock.setData(finalLockFileData);
		}
		return routeLocked;
	}


	@Override
	protected void doUnlock(RouteLock lock) {
		LockFileData lockFileData = (LockFileData) lock.getData();
		if (lockFileData != null) {
			try {
				// Read file
				MappedByteBuffer byteBuffer = lockFileData.getByteBuffer();
				read(byteBuffer, lock);
				long now = getCurrentTimestamp();
				if (lock.isLocked(getRuntimeId(), now)) {
					// Write file
					lock.unlock();
					write(byteBuffer, lock);
				}
			} catch (IOException e) {
				log.warn("File unlock writing failed", e);
			} finally {
				closeAllSilently(lockFileData.getFileLock(), lockFileData.getFileChannel(), lockFileData.getRandomAccessFile());
				log.debug("Clear Lock file data");
				lock.setData(null);
			}
		}
	}
	private void closeAllSilently(FileLock fileLock, Closeable ... closeables) {
		releaseFileLockSilently(fileLock);
		closeSilently(closeables);
	}

	private void closeSilently(Closeable ... closeables) {
		for(Closeable closeable: closeables) {
			if (closeable != null) {
				try {
					closeable.close();
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}
			}
		}
	}

	private void releaseFileLockSilently(FileLock fileLock) {
		if (fileLock != null) {
			try {
				fileLock.release();
			} catch (IOException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}
		}
	}

	public File getFolder() {
		return folder;
	}

	public void setFolder(File folder) {
		this.folder = folder;
	}
}
