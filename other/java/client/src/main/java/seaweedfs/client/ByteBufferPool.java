package seaweedfs.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferPool {

    private static final int MIN_BUFFER_SIZE = 1 * 1024 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(ByteBufferPool.class);

    private static final List<ByteBuffer> bufferList = new ArrayList<>();

    public static synchronized ByteBuffer request(int bufferSize) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            bufferSize = MIN_BUFFER_SIZE;
        }
        LOG.debug("requested new buffer {}", bufferSize);
        if (bufferList.isEmpty()) {
            return ByteBuffer.allocate(bufferSize);
        }
        ByteBuffer buffer = bufferList.remove(bufferList.size() - 1);
        if (buffer.capacity() >= bufferSize) {
            return buffer;
        }

        LOG.info("add new buffer from {} to {}", buffer.capacity(), bufferSize);
        bufferList.add(0, buffer);
        return ByteBuffer.allocate(bufferSize);

    }

    public static synchronized void release(ByteBuffer obj) {
        ((Buffer)obj).clear();
        bufferList.add(0, obj);
    }

}
