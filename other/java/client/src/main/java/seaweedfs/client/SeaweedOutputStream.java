package seaweedfs.client;

// adapted from org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class SeaweedOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedOutputStream.class);
    protected final boolean supportFlush = true;
    private final FilerClient filerClient;
    private final String path;
    private final int bufferSize;
    private final int maxConcurrentRequestCount;
    private final ThreadPoolExecutor threadExecutor;
    private final ExecutorCompletionService<Void> completionService;
    private final ConcurrentLinkedDeque<WriteOperation> writeOperations;
    private final boolean shouldSaveMetadata = false;
    private FilerProto.Entry.Builder entry;
    private long position; // Flushed bytes (committed to service)
    private long virtualPosition; // Total bytes written (including buffered), for getPos()
    private boolean closed;
    private volatile IOException lastError;
    private long lastFlushOffset;
    private long lastTotalAppendOffset = 0;
    private ByteBuffer buffer;
    private long outputIndex;
    private String replication = "";
    private String collection = "";
    private long totalBytesWritten = 0; // Track total bytes for debugging
    private long writeCallCount = 0; // Track number of write() calls

    public SeaweedOutputStream(FilerClient filerClient, final String fullpath) {
        this(filerClient, fullpath, "");
    }

    public SeaweedOutputStream(FilerClient filerClient, final String fullpath, final String replication) {
        this(filerClient, fullpath, null, 0, 8 * 1024 * 1024, replication);
    }

    public SeaweedOutputStream(FilerClient filerClient, final String path, FilerProto.Entry.Builder entry,
            final long position, final int bufferSize, final String replication) {
        LOG.warn("[DEBUG-2024] SeaweedOutputStream BASE constructor called: path={} position={} bufferSize={}",
                path, position, bufferSize);
        this.filerClient = filerClient;
        this.replication = replication;
        this.path = path;
        this.position = position;
        this.virtualPosition = position; // Initialize to match position
        this.closed = false;
        this.lastError = null;
        this.lastFlushOffset = 0;
        this.bufferSize = bufferSize;
        this.buffer = ByteBufferPool.request(bufferSize);
        this.writeOperations = new ConcurrentLinkedDeque<>();

        this.maxConcurrentRequestCount = Runtime.getRuntime().availableProcessors();

        this.threadExecutor = new ThreadPoolExecutor(maxConcurrentRequestCount,
                maxConcurrentRequestCount,
                120L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.completionService = new ExecutorCompletionService<>(this.threadExecutor);

        this.entry = entry;
        if (this.entry == null) {
            long now = System.currentTimeMillis() / 1000L;

            this.entry = FilerProto.Entry.newBuilder()
                    .setName(getFileName(path))
                    .setIsDirectory(false)
                    .setAttributes(FilerProto.FuseAttributes.newBuilder()
                            .setFileMode(0755)
                            .setCrtime(now)
                            .setMtime(now)
                            .clearGroupName());
        }

    }

    public void setReplication(String replication) {
        this.replication = replication;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    /**
     * Get the current position in the output stream.
     * This returns the total position including both flushed and buffered data.
     * 
     * @return current position (flushed + buffered bytes)
     */
    public synchronized long getPos() throws IOException {
        // CRITICAL FIX: Flush buffer before returning position
        // This ensures Parquet (and other clients) get accurate offsets based on committed data
        // Without this, Parquet records stale offsets and fails with EOF exceptions
        if (buffer.position() > 0) {
            if (path.contains("parquet")) {
                LOG.warn("[DEBUG-2024] getPos() FLUSHING buffer ({} bytes) before returning position, path={}",
                        buffer.position(), path.substring(path.lastIndexOf('/') + 1));
            }
            writeCurrentBufferToService();
        }
        
        if (path.contains("parquet")) {
            // Get caller info for debugging
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            String caller = "unknown";
            if (stackTrace.length > 2) {
                StackTraceElement callerElement = stackTrace[2];
                caller = callerElement.getClassName() + "." + callerElement.getMethodName() + ":"
                        + callerElement.getLineNumber();
            }

            LOG.warn(
                    "[DEBUG-2024] getPos() called by {}: returning position={} (all data flushed) totalBytesWritten={} writeCalls={} path={}",
                    caller, position, totalBytesWritten, writeCallCount,
                    path.substring(Math.max(0, path.length() - 80))); // Last 80 chars of path
        }
        return position; // Return position (now guaranteed to be accurate after flush)
    }

    public static String getParentDirectory(String path) {
        int protoIndex = path.indexOf("://");
        if (protoIndex >= 0) {
            int pathStart = path.indexOf("/", protoIndex + 3);
            path = path.substring(pathStart);
        }
        if (path.equals("/")) {
            return path;
        }
        int lastSlashIndex = path.lastIndexOf("/");
        if (lastSlashIndex == 0) {
            return "/";
        }
        return path.substring(0, lastSlashIndex);
    }

    public static String getFileName(String path) {
        if (path.indexOf("/") < 0) {
            return path;
        }
        int lastSlashIndex = path.lastIndexOf("/");
        return path.substring(lastSlashIndex + 1);
    }

    private synchronized void flushWrittenBytesToServiceInternal(final long offset) throws IOException {
        try {
            LOG.info("[DEBUG-2024] flushWrittenBytesToServiceInternal: path={} offset={} #chunks={}",
                    path, offset, entry.getChunksCount());

            // Set the file size in attributes based on our position
            // This ensures Parquet footer metadata matches what we actually wrote
            FilerProto.FuseAttributes.Builder attrBuilder = entry.getAttributes().toBuilder();
            attrBuilder.setFileSize(offset);
            entry.setAttributes(attrBuilder);

            if (path.contains("parquet") || path.contains("employees")) {
                LOG.warn(
                        "[DEBUG-2024] METADATA UPDATE: setting entry.attributes.fileSize = {} bytes | #chunks={} | path={}",
                        offset, entry.getChunksCount(), path.substring(path.lastIndexOf('/') + 1));
            }

            SeaweedWrite.writeMeta(filerClient, getParentDirectory(path), entry);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        this.lastFlushOffset = offset;
    }

    @Override
    public void write(final int byteVal) throws IOException {
        write(new byte[] { (byte) (byteVal & 0xFF) });
    }

    @Override
    public synchronized void write(final byte[] data, final int off, final int length)
            throws IOException {
        maybeThrowLastError();

        if (data == null) {
            return;
        }

        if (off < 0 || length < 0 || length > data.length - off) {
            throw new IndexOutOfBoundsException();
        }

        totalBytesWritten += length;
        writeCallCount++;
        virtualPosition += length; // Update virtual position for getPos()

        // Enhanced debug logging for ALL writes to track the exact sequence
        if (path.contains("parquet") || path.contains("employees")) {
            long beforeBufferPos = buffer.position();

            // Always log writes to see the complete pattern
            if (length >= 20 || writeCallCount >= 220 || writeCallCount % 50 == 0) {
                LOG.warn(
                        "[DEBUG-2024] WRITE #{}: {} bytes | virtualPos={} (flushed={} + buffered={}) | totalWritten={} | file={}",
                        writeCallCount, length, virtualPosition, position, beforeBufferPos,
                        totalBytesWritten, path.substring(path.lastIndexOf('/') + 1));
            }
        }

        // System.out.println(path + " write [" + (outputIndex + off) + "," +
        // ((outputIndex + off) + length) + ")");

        int currentOffset = off;
        int writableBytes = bufferSize - buffer.position();
        int numberOfBytesToWrite = length;

        // Track position before write
        long posBeforeWrite = position + buffer.position();

        while (numberOfBytesToWrite > 0) {

            if (numberOfBytesToWrite < writableBytes) {
                buffer.put(data, currentOffset, numberOfBytesToWrite);
                break;
            }

            // System.out.println(path + " [" + (outputIndex + currentOffset) + "," +
            // ((outputIndex + currentOffset) + writableBytes) + ") " + buffer.capacity());
            buffer.put(data, currentOffset, writableBytes);
            currentOffset += writableBytes;

            if (path.contains("parquet")) {
                LOG.warn(
                        "[DEBUG-2024] Buffer FLUSH: posBeforeFlush={} flushingBufferSize={} newPositionAfterFlush={} totalWritten={}",
                        posBeforeWrite, bufferSize, position + bufferSize, totalBytesWritten);
            }

            writeCurrentBufferToService();
            numberOfBytesToWrite = numberOfBytesToWrite - writableBytes;
            writableBytes = bufferSize - buffer.position();
        }

    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be
     * written out. If any data remains in the payload it is committed to the
     * service. Data is queued for writing and forced out to the service
     * before the call returns.
     */
    @Override
    public void flush() throws IOException {
        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] flush() CALLED: supportFlush={} virtualPos={} flushedPos={} buffer.position()={} totalWritten={} path={}",
                    supportFlush, virtualPosition, position, buffer.position(), totalBytesWritten,
                    path.substring(path.lastIndexOf('/') + 1));
        }

        if (supportFlush) {
            flushInternalAsync();
        }
    }

    /**
     * Force all data in the output stream to be written to Azure storage.
     * Wait to return until this is complete. Close the access to the stream and
     * shutdown the upload thread pool.
     * If the blob was created, its lease will be released.
     * Any error encountered caught in threads and stored will be rethrown here
     * after cleanup.
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        int bufferPosBeforeFlush = buffer.position();
        LOG.info(
                "[DEBUG-2024] close START: path={} virtualPos={} flushedPos={} buffer.position()={} totalBytesWritten={} writeCalls={}",
                path, virtualPosition, position, bufferPosBeforeFlush, totalBytesWritten, writeCallCount);
        try {
            flushInternal();
            threadExecutor.shutdown();
            LOG.info(
                    "[DEBUG-2024] close END: path={} virtualPos={} flushedPos={} totalBytesWritten={} writeCalls={} (buffer had {} bytes)",
                    path, virtualPosition, position, totalBytesWritten, writeCallCount, bufferPosBeforeFlush);

            // Special logging for employees directory files (to help CI download timing)
            if (path.contains("/test-spark/employees/") && path.endsWith(".parquet")) {
                String filename = path.substring(path.lastIndexOf('/') + 1);
                // Log filename, size, AND chunk IDs for direct volume download
                StringBuilder chunkInfo = new StringBuilder();
                for (int i = 0; i < entry.getChunksCount(); i++) {
                    FilerProto.FileChunk chunk = entry.getChunks(i);
                    if (i > 0)
                        chunkInfo.append(",");
                    chunkInfo.append(chunk.getFileId());
                }
                LOG.warn("=== PARQUET FILE WRITTEN TO EMPLOYEES: {} ({} bytes) CHUNKS: [{}] ===",
                        filename, position, chunkInfo.toString());
            }
        } finally {
            lastError = new IOException("Stream is closed!");
            ByteBufferPool.release(buffer);
            buffer = null;
            closed = true;
            writeOperations.clear();
            if (!threadExecutor.isShutdown()) {
                threadExecutor.shutdownNow();
            }
        }

    }

    private synchronized void writeCurrentBufferToService() throws IOException {
        int bufferPos = buffer.position();
        long positionBefore = position;

        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] writeCurrentBufferToService START: buffer.position()={} currentFlushedPosition={} totalWritten={} path={}",
                    bufferPos, position, totalBytesWritten, path.substring(path.lastIndexOf('/') + 1));
        }

        if (bufferPos == 0) {
            if (path.contains("parquet") || path.contains("employees")) {
                LOG.warn("[DEBUG-2024]   -> Skipping: buffer is empty");
            }
            return;
        }

        int written = submitWriteBufferToService(buffer, position);
        position += written;

        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] writeCurrentBufferToService END: submitted {} bytes, flushedPosition {} -> {}, totalWritten={} path={}",
                    written, positionBefore, position, totalBytesWritten, path.substring(path.lastIndexOf('/') + 1));
        }

        buffer = ByteBufferPool.request(bufferSize);

    }

    private synchronized int submitWriteBufferToService(final ByteBuffer bufferToWrite, final long writePosition)
            throws IOException {

        ((Buffer) bufferToWrite).flip();
        int bytesLength = bufferToWrite.limit() - bufferToWrite.position();

        if (threadExecutor.getQueue().size() >= maxConcurrentRequestCount) {
            waitForTaskToComplete();
        }
        final Future<Void> job = completionService.submit(() -> {
            // System.out.println(path + " is going to save [" + (writePosition) + "," +
            // ((writePosition) + bytesLength) + ")");
            SeaweedWrite.writeData(entry, replication, collection, filerClient, writePosition, bufferToWrite.array(),
                    bufferToWrite.position(), bufferToWrite.limit(), path);
            // System.out.println(path + " saved [" + (writePosition) + "," +
            // ((writePosition) + bytesLength) + ")");
            ByteBufferPool.release(bufferToWrite);
            return null;
        });

        writeOperations.add(new WriteOperation(job, writePosition, bytesLength));

        // Try to shrink the queue
        shrinkWriteOperationQueue();

        return bytesLength;

    }

    private void waitForTaskToComplete() throws IOException {
        boolean completed;
        for (completed = false; completionService.poll() != null; completed = true) {
            // keep polling until there is no data
        }

        if (!completed) {
            try {
                completionService.take();
            } catch (InterruptedException e) {
                lastError = (IOException) new InterruptedIOException(e.toString()).initCause(e);
                throw lastError;
            }
        }
    }

    private void maybeThrowLastError() throws IOException {
        if (lastError != null) {
            throw lastError;
        }
    }

    /**
     * Try to remove the completed write operations from the beginning of write
     * operation FIFO queue.
     */
    private synchronized void shrinkWriteOperationQueue() throws IOException {
        try {
            while (writeOperations.peek() != null && writeOperations.peek().task.isDone()) {
                writeOperations.peek().task.get();
                lastTotalAppendOffset += writeOperations.peek().length;
                writeOperations.remove();
            }
        } catch (Exception e) {
            lastError = new IOException(e);
            throw lastError;
        }
    }

    protected synchronized void flushInternal() throws IOException {
        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] flushInternal() START: virtualPos={} flushedPos={} buffer.position()={} totalWritten={} path={}",
                    virtualPosition, position, buffer.position(), totalBytesWritten,
                    path.substring(path.lastIndexOf('/') + 1));
        }

        maybeThrowLastError();
        writeCurrentBufferToService();
        flushWrittenBytesToService();

        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] flushInternal() END: virtualPos={} flushedPos={} buffer.position()={} totalWritten={} path={}",
                    virtualPosition, position, buffer.position(), totalBytesWritten,
                    path.substring(path.lastIndexOf('/') + 1));
        }
    }

    protected synchronized void flushInternalAsync() throws IOException {
        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] flushInternalAsync() START: virtualPos={} flushedPos={} buffer.position()={} totalWritten={} path={}",
                    virtualPosition, position, buffer.position(), totalBytesWritten,
                    path.substring(path.lastIndexOf('/') + 1));
        }

        maybeThrowLastError();
        writeCurrentBufferToService();
        flushWrittenBytesToServiceAsync();

        if (path.contains("parquet") || path.contains("employees")) {
            LOG.warn(
                    "[DEBUG-2024] flushInternalAsync() END: virtualPos={} flushedPos={} buffer.position()={} totalWritten={} path={}",
                    virtualPosition, position, buffer.position(), totalBytesWritten,
                    path.substring(path.lastIndexOf('/') + 1));
        }
    }

    private synchronized void flushWrittenBytesToService() throws IOException {
        for (WriteOperation writeOperation : writeOperations) {
            try {
                writeOperation.task.get();
            } catch (Exception ex) {
                lastError = new IOException(ex);
                throw lastError;
            }
        }
        LOG.debug("flushWrittenBytesToService: {} position:{}", path, position);
        flushWrittenBytesToServiceInternal(position);
    }

    private synchronized void flushWrittenBytesToServiceAsync() throws IOException {
        shrinkWriteOperationQueue();

        if (this.lastTotalAppendOffset > this.lastFlushOffset) {
            this.flushWrittenBytesToServiceInternal(this.lastTotalAppendOffset);
        }
    }

    private static class WriteOperation {
        private final Future<Void> task;
        private final long startOffset;
        private final long length;

        WriteOperation(final Future<Void> task, final long startOffset, final long length) {
            this.task = task;
            this.startOffset = startOffset;
            this.length = length;
        }
    }

}
