package seaweed.hdfs;

// adapted from org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedWrite;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static seaweed.hdfs.SeaweedFileSystemStore.getParentDirectory;

public class SeaweedOutputStream extends OutputStream implements Syncable, StreamCapabilities {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedOutputStream.class);

    private final FilerGrpcClient filerGrpcClient;
    private final Path path;
    private final int bufferSize;
    private final int maxConcurrentRequestCount;
    private final ThreadPoolExecutor threadExecutor;
    private final ExecutorCompletionService<Void> completionService;
    private FilerProto.Entry.Builder entry;
    private long position;
    private boolean closed;
    private boolean supportFlush = true;
    private volatile IOException lastError;
    private long lastFlushOffset;
    private long lastTotalAppendOffset = 0;
    private byte[] buffer;
    private int bufferIndex;
    private ConcurrentLinkedDeque<WriteOperation> writeOperations;
    private String replication = "000";

    public SeaweedOutputStream(FilerGrpcClient filerGrpcClient, final Path path, FilerProto.Entry.Builder entry,
                               final long position, final int bufferSize, final String replication) {
        this.filerGrpcClient = filerGrpcClient;
        this.replication = replication;
        this.path = path;
        this.position = position;
        this.closed = false;
        this.lastError = null;
        this.lastFlushOffset = 0;
        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];
        this.bufferIndex = 0;
        this.writeOperations = new ConcurrentLinkedDeque<>();

        this.maxConcurrentRequestCount = 4 * Runtime.getRuntime().availableProcessors();

        this.threadExecutor
            = new ThreadPoolExecutor(maxConcurrentRequestCount,
            maxConcurrentRequestCount,
            10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());
        this.completionService = new ExecutorCompletionService<>(this.threadExecutor);

        this.entry = entry;

    }

    private synchronized void flushWrittenBytesToServiceInternal(final long offset) throws IOException {

        LOG.debug("SeaweedWrite.writeMeta path: {} entry:{}", path, entry);

        try {
            SeaweedWrite.writeMeta(filerGrpcClient, getParentDirectory(path), entry);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        this.lastFlushOffset = offset;
    }

    @Override
    public void write(final int byteVal) throws IOException {
        write(new byte[]{(byte) (byteVal & 0xFF)});
    }

    @Override
    public synchronized void write(final byte[] data, final int off, final int length)
        throws IOException {
        maybeThrowLastError();

        Preconditions.checkArgument(data != null, "null data");

        if (off < 0 || length < 0 || length > data.length - off) {
            throw new IndexOutOfBoundsException();
        }

        int currentOffset = off;
        int writableBytes = bufferSize - bufferIndex;
        int numberOfBytesToWrite = length;

        while (numberOfBytesToWrite > 0) {
            if (writableBytes <= numberOfBytesToWrite) {
                System.arraycopy(data, currentOffset, buffer, bufferIndex, writableBytes);
                bufferIndex += writableBytes;
                writeCurrentBufferToService();
                currentOffset += writableBytes;
                numberOfBytesToWrite = numberOfBytesToWrite - writableBytes;
            } else {
                System.arraycopy(data, currentOffset, buffer, bufferIndex, numberOfBytesToWrite);
                bufferIndex += numberOfBytesToWrite;
                numberOfBytesToWrite = 0;
            }

            writableBytes = bufferSize - bufferIndex;
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
        if (supportFlush) {
            flushInternalAsync();
        }
    }

    /**
     * Similar to posix fsync, flush out the data in client's user buffer
     * all the way to the disk device (but the disk may have it in its cache).
     *
     * @throws IOException if error occurs
     */
    @Override
    public void hsync() throws IOException {
        if (supportFlush) {
            flushInternal();
        }
    }

    /**
     * Flush out the data in client's user buffer. After the return of
     * this call, new readers will see the data.
     *
     * @throws IOException if any error occurs
     */
    @Override
    public void hflush() throws IOException {
        if (supportFlush) {
            flushInternal();
        }
    }

    /**
     * Query the stream for a specific capability.
     *
     * @param capability string to query the stream support for.
     * @return true for hsync and hflush.
     */
    @Override
    public boolean hasCapability(String capability) {
        switch (capability.toLowerCase(Locale.ENGLISH)) {
            case StreamCapabilities.HSYNC:
            case StreamCapabilities.HFLUSH:
                return supportFlush;
            default:
                return false;
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

        LOG.debug("close path: {}", path);
        try {
            flushInternal();
            threadExecutor.shutdown();
        } finally {
            lastError = new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
            buffer = null;
            bufferIndex = 0;
            closed = true;
            writeOperations.clear();
            if (!threadExecutor.isShutdown()) {
                threadExecutor.shutdownNow();
            }
        }
    }

    private synchronized void writeCurrentBufferToService() throws IOException {
        if (bufferIndex == 0) {
            return;
        }

        final byte[] bytes = buffer;
        final int bytesLength = bufferIndex;

        buffer = new byte[bufferSize];
        bufferIndex = 0;
        final long offset = position;
        position += bytesLength;

        if (threadExecutor.getQueue().size() >= maxConcurrentRequestCount * 2) {
            waitForTaskToComplete();
        }

        final Future<Void> job = completionService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                // originally: client.append(path, offset, bytes, 0, bytesLength);
                SeaweedWrite.writeData(entry, replication, filerGrpcClient, offset, bytes, 0, bytesLength);
                return null;
            }
        });

        writeOperations.add(new WriteOperation(job, offset, bytesLength));

        // Try to shrink the queue
        shrinkWriteOperationQueue();
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

    private synchronized void flushInternal() throws IOException {
        maybeThrowLastError();
        writeCurrentBufferToService();
        flushWrittenBytesToService();
    }

    private synchronized void flushInternalAsync() throws IOException {
        maybeThrowLastError();
        writeCurrentBufferToService();
        flushWrittenBytesToServiceAsync();
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
            Preconditions.checkNotNull(task, "task");
            Preconditions.checkArgument(startOffset >= 0, "startOffset");
            Preconditions.checkArgument(length >= 0, "length");

            this.task = task;
            this.startOffset = startOffset;
            this.length = length;
        }
    }

}
