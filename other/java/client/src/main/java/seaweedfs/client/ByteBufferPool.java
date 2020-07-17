package seaweedfs.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferPool {

    static List<ByteBuffer> bufferList = new ArrayList<>();

    public static synchronized ByteBuffer request(int bufferSize) {
        if (bufferList.isEmpty()) {
            return ByteBuffer.allocate(bufferSize);
        }
        return bufferList.remove(bufferList.size()-1);
    }

    public static synchronized void release(ByteBuffer obj) {
        bufferList.add(obj);
    }

}
