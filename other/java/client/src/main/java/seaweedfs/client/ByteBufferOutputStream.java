package seaweedfs.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
    private final ByteBuffer buf;

    public ByteBufferOutputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public void write(int b) throws IOException {
        this.buf.put((byte)b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        this.buf.put(b, off, len);
    }
}
