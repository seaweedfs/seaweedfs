package seaweed.hdfs;

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferedByteBufferReadableInputStream extends BufferedFSInputStream implements ByteBufferReadable {

    public BufferedByteBufferReadableInputStream(FSInputStream in, int size) {
        super(in, size);
        if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)) {
            throw new IllegalArgumentException("In is not an instance of Seekable or PositionedReadable");
        }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        if (this.in instanceof ByteBufferReadable) {
            return ((ByteBufferReadable)this.in).read(buf);
        } else {
            throw new UnsupportedOperationException("Byte-buffer read unsupported by input stream");
        }
    }
}
