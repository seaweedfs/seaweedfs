package seaweed.hdfs;

// based on org.apache.hadoop.fs.azurebfs.services.AbfsInputStream

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedRead;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SeaweedInputStream extends FSInputStream implements ByteBufferReadable {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedInputStream.class);

    private final FilerGrpcClient filerGrpcClient;
    private final Statistics statistics;
    private final String path;
    private final FilerProto.Entry entry;
    private final List<SeaweedRead.VisibleInterval> visibleIntervalList;
    private final long contentLength;

    private long position = 0;  // cursor of the file

    private boolean closed = false;

    public SeaweedInputStream(
            final FilerGrpcClient filerGrpcClient,
            final Statistics statistics,
            final String path,
            final FilerProto.Entry entry) throws IOException {
        this.filerGrpcClient = filerGrpcClient;
        this.statistics = statistics;
        this.path = path;
        this.entry = entry;
        this.contentLength = SeaweedRead.fileSize(entry);

        this.visibleIntervalList = SeaweedRead.nonOverlappingVisibleIntervals(filerGrpcClient, entry.getChunksList());

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
    @Override
    public synchronized int read(ByteBuffer buf) throws IOException {

        if (position < 0) {
            throw new IllegalArgumentException("attempting to read from negative offset");
        }
        if (position >= contentLength) {
            return -1;  // Hadoop prefers -1 to EOFException
        }

        long bytesRead = 0;
        int len = buf.remaining();
        int start = (int) this.position;
        if (start+len <= entry.getContent().size()) {
            entry.getContent().substring(start, start+len).copyTo(buf);
        } else {
            bytesRead = SeaweedRead.read(this.filerGrpcClient, this.visibleIntervalList, this.position, buf, SeaweedRead.fileSize(entry));
        }

        if (bytesRead > Integer.MAX_VALUE) {
            throw new IOException("Unexpected Content-Length");
        }

        if (bytesRead > 0) {
            this.position += bytesRead;
            if (statistics != null) {
                statistics.incrementBytesRead(bytesRead);
            }
        }

        return (int) bytesRead;
    }

    /**
     * Seek to given position in stream.
     *
     * @param n position to seek to
     * @throws IOException  if there is an error
     * @throws EOFException if attempting to seek past end of file
     */
    @Override
    public synchronized void seek(long n) throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        if (n < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (n > contentLength) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }

        this.position = n;

    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        if (this.position == contentLength) {
            if (n > 0) {
                throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
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
            throw new IOException(
                    FSExceptionMessages.STREAM_IS_CLOSED);
        }
        final long remaining = this.contentLength - this.getPos();
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
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        return contentLength;
    }

    /**
     * Return the current offset from the start of the file
     *
     * @throws IOException throws {@link IOException} if there is an error
     */
    @Override
    public synchronized long getPos() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        return position;
    }

    /**
     * Seeks a different copy of the data.  Returns true if
     * found a new source, false otherwise.
     *
     * @throws IOException throws {@link IOException} if there is an error
     */
    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
    }

    /**
     * Not supported by this stream. Throws {@link UnsupportedOperationException}
     *
     * @param readlimit ignored
     */
    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    /**
     * Not supported by this stream. Throws {@link UnsupportedOperationException}
     */
    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    /**
     * gets whether mark and reset are supported by {@code ADLFileInputStream}. Always returns false.
     *
     * @return always {@code false}
     */
    @Override
    public boolean markSupported() {
        return false;
    }
}
