package seaweed.hdfs;

// adapted from org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.ByteBufferPool;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedWrite;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

import static seaweed.hdfs.SeaweedFileSystemStore.getParentDirectory;

public class SeaweedOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedOutputStream.class);

    private final FilerGrpcClient filerGrpcClient;
    private final Path path;
    private final int bufferSize;
    private final int maxConcurrentRequestCount;
    private final ThreadPoolExecutor threadExecutor;
    private final ExecutorCompletionService<Void> completionService;
    private final FilerProto.Entry.Builder entry;
    private final boolean supportFlush = false; // true;
    private final ConcurrentLinkedDeque<WriteOperation> writeOperations;
    private long position;
    private boolean closed;
    private volatile IOException lastError;
    private long lastFlushOffset;
    private long lastTotalAppendOffset = 0;
    private ByteBuffer buffer;
    private long outputIndex;
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
        this.buffer = ByteBufferPool.request(bufferSize);
        this.outputIndex = 0;
        this.writeOperations = new ConcurrentLinkedDeque<>();

        this.maxConcurrentRequestCount = Runtime.getRuntime().availableProcessors();

        this.threadExecutor
                = new ThreadPoolExecutor(maxConcurrentRequestCount,
                maxConcurrentRequestCount,
                120L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.completionService = new ExecutorCompletionService<>(this.threadExecutor);

        this.entry = entry;

    }

    private synchronized void flushWrittenBytesToServiceInternal(final long offset) throws IOException {
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

        // System.out.println(path + " write [" + (outputIndex + off) + "," + ((outputIndex + off) + length) + ")");

        int currentOffset = off;
        int writableBytes = bufferSize - buffer.position();
        int numberOfBytesToWrite = length;

        while (numberOfBytesToWrite > 0) {

            if (numberOfBytesToWrite < writableBytes) {
                buffer.put(data, currentOffset, numberOfBytesToWrite);
                outputIndex += numberOfBytesToWrite;
                break;
            }

            // System.out.println(path + "     [" + (outputIndex + currentOffset) + "," + ((outputIndex + currentOffset) + writableBytes) + ") " + buffer.capacity());
            buffer.put(data, currentOffset, writableBytes);
            outputIndex += writableBytes;
            currentOffset += writableBytes;
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

        LOG.debug("close path: {}", path);
        try {
            flushInternal();
            threadExecutor.shutdown();
        } finally {
            lastError = new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
            ByteBufferPool.release(buffer);
            buffer = null;
            outputIndex = 0;
            closed = true;
            writeOperations.clear();
            if (!threadExecutor.isShutdown()) {
                threadExecutor.shutdownNow();
            }
        }
    }

    private synchronized void writeCurrentBufferToService() throws IOException {
        if (buffer.position() == 0) {
            return;
        }

        position += submitWriteBufferToService(buffer, position);

        buffer = ByteBufferPool.request(bufferSize);

    }

    private synchronized int submitWriteBufferToService(final ByteBuffer bufferToWrite, final long writePosition) throws IOException {

        bufferToWrite.flip();
        int bytesLength = bufferToWrite.limit() - bufferToWrite.position();

        if (threadExecutor.getQueue().size() >= maxConcurrentRequestCount) {
            waitForTaskToComplete();
        }
        final Future<Void> job = completionService.submit(() -> {
            // System.out.println(path + " is going to save [" + (writePosition) + "," + ((writePosition) + bytesLength) + ")");
            SeaweedWrite.writeData(entry, replication, filerGrpcClient, writePosition, bufferToWrite.array(), bufferToWrite.position(), bufferToWrite.limit(), path.toUri().getPath());
            // System.out.println(path + " saved [" + (writePosition) + "," + ((writePosition) + bytesLength) + ")");
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
