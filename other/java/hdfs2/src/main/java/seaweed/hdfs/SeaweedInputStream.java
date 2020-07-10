package seaweed.hdfs;

// based on org.apache.hadoop.fs.azurebfs.services.AbfsInputStream

import com.google.common.base.Preconditions;
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
import java.util.List;

public class SeaweedInputStream extends FSInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedInputStream.class);

    private final FilerGrpcClient filerGrpcClient;
    private final Statistics statistics;
    private final String path;
    private final FilerProto.Entry entry;
    private final List<SeaweedRead.VisibleInterval> visibleIntervalList;
    private final long contentLength;
    private final int bufferSize; // default buffer size
    private final int readAheadQueueDepth;         // initialized in constructor
    private final boolean readAheadEnabled; // whether enable readAhead;

    private byte[] buffer = null;            // will be initialized on first use

    private long fCursor = 0;  // cursor of buffer within file - offset of next byte to read from remote server
    private long fCursorAfterLastRead = -1;
    private int bCursor = 0;   // cursor of read within buffer - offset of next byte to be returned from buffer
    private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
    //                                                      of valid bytes in buffer)
    private boolean closed = false;

    public SeaweedInputStream(
        final FilerGrpcClient filerGrpcClient,
        final Statistics statistics,
        final String path,
        final FilerProto.Entry entry,
        final int bufferSize,
        final int readAheadQueueDepth) {
        this.filerGrpcClient = filerGrpcClient;
        this.statistics = statistics;
        this.path = path;
        this.entry = entry;
        this.contentLength = SeaweedRead.totalSize(entry.getChunksList());
        this.bufferSize = bufferSize;
        this.readAheadQueueDepth = (readAheadQueueDepth >= 0) ? readAheadQueueDepth : Runtime.getRuntime().availableProcessors();
        this.readAheadEnabled = true;

        this.visibleIntervalList = SeaweedRead.nonOverlappingVisibleIntervals(entry.getChunksList());

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
    public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
        int currentOff = off;
        int currentLen = len;
        int lastReadBytes;
        int totalReadBytes = 0;
        do {
            lastReadBytes = readOneBlock(b, currentOff, currentLen);
            if (lastReadBytes > 0) {
                currentOff += lastReadBytes;
                currentLen -= lastReadBytes;
                totalReadBytes += lastReadBytes;
            }
            if (currentLen <= 0 || currentLen > b.length - currentOff) {
                break;
            }
        } while (lastReadBytes > 0);
        return totalReadBytes > 0 ? totalReadBytes : lastReadBytes;
    }

    private int readOneBlock(final byte[] b, final int off, final int len) throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }

        Preconditions.checkNotNull(b);

        if (len == 0) {
            return 0;
        }

        if (this.available() == 0) {
            return -1;
        }

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        //If buffer is empty, then fill the buffer.
        if (bCursor == limit) {
            //If EOF, then return -1
            if (fCursor >= contentLength) {
                return -1;
            }

            long bytesRead = 0;
            //reset buffer to initial state - i.e., throw away existing data
            bCursor = 0;
            limit = 0;
            if (buffer == null) {
                buffer = new byte[bufferSize];
            }

            // Enable readAhead when reading sequentially
            if (-1 == fCursorAfterLastRead || fCursorAfterLastRead == fCursor || b.length >= bufferSize) {
                bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
            } else {
                bytesRead = readInternal(fCursor, buffer, 0, b.length, true);
            }

            if (bytesRead == -1) {
                return -1;
            }

            limit += bytesRead;
            fCursor += bytesRead;
            fCursorAfterLastRead = fCursor;
        }

        //If there is anything in the buffer, then return lesser of (requested bytes) and (bytes in buffer)
        //(bytes returned may be less than requested)
        int bytesRemaining = limit - bCursor;
        int bytesToRead = Math.min(len, bytesRemaining);
        System.arraycopy(buffer, bCursor, b, off, bytesToRead);
        bCursor += bytesToRead;
        if (statistics != null) {
            statistics.incrementBytesRead(bytesToRead);
        }
        return bytesToRead;
    }


    private int readInternal(final long position, final byte[] b, final int offset, final int length,
                             final boolean bypassReadAhead) throws IOException {
        if (readAheadEnabled && !bypassReadAhead) {
            // try reading from read-ahead
            if (offset != 0) {
                throw new IllegalArgumentException("readahead buffers cannot have non-zero buffer offsets");
            }
            int receivedBytes;

            // queue read-aheads
            int numReadAheads = this.readAheadQueueDepth;
            long nextSize;
            long nextOffset = position;
            while (numReadAheads > 0 && nextOffset < contentLength) {
                nextSize = Math.min((long) bufferSize, contentLength - nextOffset);
                ReadBufferManager.getBufferManager().queueReadAhead(this, nextOffset, (int) nextSize);
                nextOffset = nextOffset + nextSize;
                numReadAheads--;
            }

            // try reading from buffers first
            receivedBytes = ReadBufferManager.getBufferManager().getBlock(this, position, length, b);
            if (receivedBytes > 0) {
                return receivedBytes;
            }

            // got nothing from read-ahead, do our own read now
            receivedBytes = readRemote(position, b, offset, length);
            return receivedBytes;
        } else {
            return readRemote(position, b, offset, length);
        }
    }

    int readRemote(long position, byte[] b, int offset, int length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("attempting to read from negative offset");
        }
        if (position >= contentLength) {
            return -1;  // Hadoop prefers -1 to EOFException
        }
        if (b == null) {
            throw new IllegalArgumentException("null byte array passed in to read() method");
        }
        if (offset >= b.length) {
            throw new IllegalArgumentException("offset greater than length of array");
        }
        if (length < 0) {
            throw new IllegalArgumentException("requested read length is less than zero");
        }
        if (length > (b.length - offset)) {
            throw new IllegalArgumentException("requested read length is more than will fit after requested offset in buffer");
        }

        long bytesRead = SeaweedRead.read(filerGrpcClient, visibleIntervalList, position, b, offset, length);
        if (bytesRead > Integer.MAX_VALUE) {
            throw new IOException("Unexpected Content-Length");
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

        if (n >= fCursor - limit && n <= fCursor) { // within buffer
            bCursor = (int) (n - (fCursor - limit));
            return;
        }

        // next read will read from here
        fCursor = n;

        //invalidate buffer
        limit = 0;
        bCursor = 0;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        long currentPos = getPos();
        if (currentPos == contentLength) {
            if (n > 0) {
                throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
            }
        }
        long newPos = currentPos + n;
        if (newPos < 0) {
            newPos = 0;
            n = newPos - currentPos;
        }
        if (newPos > contentLength) {
            newPos = contentLength;
            n = newPos - currentPos;
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
        return fCursor - limit + bCursor;
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
        buffer = null; // de-reference the buffer so it can be GC'ed sooner
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
