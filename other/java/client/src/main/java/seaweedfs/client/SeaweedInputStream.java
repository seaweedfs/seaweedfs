package seaweedfs.client;

// based on org.apache.hadoop.fs.azurebfs.services.AbfsInputStream

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class SeaweedInputStream extends InputStream {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedInputStream.class);
    private static final IOException EXCEPTION_STREAM_IS_CLOSED = new IOException("Stream is closed!");

    private final FilerClient filerClient;
    private final String path;
    private final List<SeaweedRead.VisibleInterval> visibleIntervalList;
    private final long contentLength;
    private FilerProto.Entry entry;

    private long position = 0;  // cursor of the file

    private boolean closed = false;

    public SeaweedInputStream(
            final FilerClient filerClient,
            final String fullpath) throws IOException {
        this.path = fullpath;
        this.filerClient = filerClient;
        this.entry = filerClient.lookupEntry(
                SeaweedOutputStream.getParentDirectory(fullpath),
                SeaweedOutputStream.getFileName(fullpath));
        if (entry == null) {
            throw new FileNotFoundException();
        }

        if (RemoteUtil.isInRemoteOnly(entry)) {
            entry = RemoteUtil.downloadRemoteEntry(filerClient, fullpath, entry);
        }

        this.contentLength = SeaweedRead.fileSize(entry);

        this.visibleIntervalList = SeaweedRead.nonOverlappingVisibleIntervals(filerClient, entry.getChunksList());

        LOG.debug("new path:{} entry:{} visibleIntervalList:{}", path, entry, visibleIntervalList);

    }

    public SeaweedInputStream(
            final FilerClient filerClient,
            final String path,
            final FilerProto.Entry entry) throws IOException {
        this.filerClient = filerClient;
        this.path = path;
        this.entry = entry;

        if (RemoteUtil.isInRemoteOnly(entry)) {
            this.entry = RemoteUtil.downloadRemoteEntry(filerClient, path, entry);
        }

        this.contentLength = SeaweedRead.fileSize(entry);

        this.visibleIntervalList = SeaweedRead.nonOverlappingVisibleIntervals(filerClient, entry.getChunksList());

        LOG.debug("new path:{} entry:{} visibleIntervalList:{}", path, entry, visibleIntervalList);

    }

    public String getPath() {
        return path;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int numberOfBytesRead = read(b, 0, 1);
        if (numberOfBytesRead < 0) {
            return -1;
        } else {
            return (b[0] & 0xFF);
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {

        if (b == null) {
            throw new IllegalArgumentException("null byte array passed in to read() method");
        }
        if (off >= b.length) {
            throw new IllegalArgumentException("offset greater than length of array");
        }
        if (len < 0) {
            throw new IllegalArgumentException("requested read length is less than zero");
        }
        if (len > (b.length - off)) {
            throw new IllegalArgumentException("requested read length is more than will fit after requested offset in buffer");
        }

        ByteBuffer buf = ByteBuffer.wrap(b, off, len);
        return read(buf);

    }

    // implement ByteBufferReadable
    public synchronized int read(ByteBuffer buf) throws IOException {

        if (position < 0) {
            throw new IllegalArgumentException("attempting to read from negative offset");
        }
        if (position >= contentLength) {
            return -1;  // Hadoop prefers -1 to EOFException
        }

        long bytesRead = 0;
        int len = buf.remaining();
        if (this.position< Integer.MAX_VALUE && (this.position + len )<= entry.getContent().size()) {
            entry.getContent().substring((int)this.position, (int)(this.position + len)).copyTo(buf);
        } else {
            bytesRead = SeaweedRead.read(this.filerClient, this.visibleIntervalList, this.position, buf, SeaweedRead.fileSize(entry));
        }

        if (bytesRead > Integer.MAX_VALUE) {
            throw new IOException("Unexpected Content-Length");
        }

        if (bytesRead > 0) {
            this.position += bytesRead;
        }

        return (int) bytesRead;
    }

    public synchronized void seek(long n) throws IOException {
        if (closed) {
            throw EXCEPTION_STREAM_IS_CLOSED;
        }
        if (n < 0) {
            throw new EOFException("Cannot seek to a negative offset");
        }
        if (n > contentLength) {
            throw new EOFException("Attempted to seek or read past the end of the file");
        }
        this.position = n;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (closed) {
            throw EXCEPTION_STREAM_IS_CLOSED;
        }
        if (this.position == contentLength) {
            if (n > 0) {
                throw new EOFException("Attempted to seek or read past the end of the file");
            }
        }
        long newPos = this.position + n;
        if (newPos < 0) {
            newPos = 0;
            n = newPos - this.position;
        }
        if (newPos > contentLength) {
            newPos = contentLength;
            n = newPos - this.position;
        }
        seek(newPos);
        return n;
    }

    /**
     * Return the size of the remaining available bytes
     * if the size is less than or equal to {@link Integer#MAX_VALUE},
     * otherwise, return {@link Integer#MAX_VALUE}.
     * <p>
     * This is to match the behavior of DFSInputStream.available(),
     * which some clients may rely on (HBase write-ahead log reading in
     * particular).
     */
    @Override
    public synchronized int available() throws IOException {
        if (closed) {
            throw EXCEPTION_STREAM_IS_CLOSED;
        }
        final long remaining = this.contentLength - this.position;
        return remaining <= Integer.MAX_VALUE
                ? (int) remaining : Integer.MAX_VALUE;
    }

    /**
     * Returns the length of the file that this stream refers to. Note that the length returned is the length
     * as of the time the Stream was opened. Specifically, if there have been subsequent appends to the file,
     * they wont be reflected in the returned length.
     *
     * @return length of the file.
     * @throws IOException if the stream is closed
     */
    public long length() throws IOException {
        if (closed) {
            throw EXCEPTION_STREAM_IS_CLOSED;
        }
        return contentLength;
    }

    public synchronized long getPos() throws IOException {
        if (closed) {
            throw EXCEPTION_STREAM_IS_CLOSED;
        }
        return position;
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
    }

}
