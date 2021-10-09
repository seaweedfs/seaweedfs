package seaweedfs.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class ChunkCache {

    private Cache<String, byte[]> cache = null;

    public ChunkCache(int maxEntries) {
        if (maxEntries == 0) {
            return;
        }
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maxEntries)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
    }

    public byte[] getChunk(String fileId) {
        if (this.cache == null) {
            return null;
        }
        return this.cache.getIfPresent(fileId);
    }

    public void setChunk(String fileId, byte[] data) {
        if (this.cache == null) {
            return;
        }
        this.cache.put(fileId, data);
    }

}
