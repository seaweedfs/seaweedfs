package seaweed.hdfs;

import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerProto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Atomic output stream for Parquet files.
 * 
 * Buffers all writes in memory and writes atomically on close().
 * This ensures that getPos() always returns accurate positions that match
 * the final file layout, which is required for Parquet's footer metadata.
 */
public class SeaweedAtomicOutputStream extends SeaweedHadoopOutputStream implements Syncable {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedAtomicOutputStream.class);

    private final ByteArrayOutputStream memoryBuffer;
    private final String filePath;
    private boolean closed = false;

    public SeaweedAtomicOutputStream(FilerClient filerClient, String path, FilerProto.Entry.Builder entry,
            long position, int maxBufferSize, String replication) {
        super(filerClient, path, entry, position, maxBufferSize, replication);
        this.filePath = path;
        this.memoryBuffer = new ByteArrayOutputStream(maxBufferSize);
        LOG.info("[ATOMIC] Created atomic output stream for: {} (maxBuffer={})", path, maxBufferSize);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        memoryBuffer.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        memoryBuffer.write(b, off, len);
    }

    @Override
    public synchronized long getPos() throws IOException {
        // Return the current size of the memory buffer
        // This is always accurate since nothing is flushed until close()
        long pos = memoryBuffer.size();
        
        // Log getPos() calls around the problematic positions
        if (pos >= 470 && pos <= 476) {
            LOG.error("[ATOMIC-GETPOS] getPos() returning pos={}", pos);
        }
        
        return pos;
    }

    @Override
    public synchronized void flush() throws IOException {
        // No-op for atomic writes - everything is flushed on close()
        LOG.debug("[ATOMIC] flush() called (no-op for atomic writes)");
    }

    @Override
    public synchronized void hsync() throws IOException {
        // No-op for atomic writes
        LOG.debug("[ATOMIC] hsync() called (no-op for atomic writes)");
    }

    @Override
    public synchronized void hflush() throws IOException {
        // No-op for atomic writes
        LOG.debug("[ATOMIC] hflush() called (no-op for atomic writes)");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            byte[] data = memoryBuffer.toByteArray();
            int size = data.length;

            LOG.info("[ATOMIC] Closing atomic stream: {} ({} bytes buffered)", filePath, size);

            if (size > 0) {
                // Write all data at once using the parent's write method
                super.write(data, 0, size);
            }

            // Now close the parent stream which will flush and write metadata
            super.close();

            LOG.info("[ATOMIC] Successfully wrote {} bytes atomically to: {}", size, filePath);
        } finally {
            closed = true;
            memoryBuffer.reset();
        }
    }
}
