package seaweed.hdfs;

// based on org.apache.hadoop.fs.azurebfs.services.AbfsInputStream

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SeaweedHadoopInputStream extends FSInputStream implements ByteBufferReadable {

    private final SeaweedInputStream seaweedInputStream;
    private final Statistics statistics;

    public SeaweedHadoopInputStream(
            final FilerClient filerClient,
            final Statistics statistics,
            final String path,
            final FilerProto.Entry entry) throws IOException {
        this.seaweedInputStream = new SeaweedInputStream(filerClient, path, entry);
        this.statistics = statistics;
    }

    @Override
    public int read() throws IOException {
        return seaweedInputStream.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        return seaweedInputStream.read(b, off, len);
    }

    // implement ByteBufferReadable
    @Override
    public synchronized int read(ByteBuffer buf) throws IOException {
        int bytesRead = seaweedInputStream.read(buf);

        if (bytesRead > 0) {
            if (statistics != null) {
                statistics.incrementBytesRead(bytesRead);
            }
        }

        return bytesRead;
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
        seaweedInputStream.seek(n);
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        return seaweedInputStream.skip(n);
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
        return seaweedInputStream.available();
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
        return seaweedInputStream.length();
    }

    /**
     * Return the current offset from the start of the file
     *
     * @throws IOException throws {@link IOException} if there is an error
     */
    @Override
    public synchronized long getPos() throws IOException {
        return seaweedInputStream.getPos();
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
        seaweedInputStream.close();
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
