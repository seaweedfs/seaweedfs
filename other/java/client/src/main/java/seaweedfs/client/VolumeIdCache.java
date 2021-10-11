package seaweedfs.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class VolumeIdCache {

    private Cache<String, FilerProto.Locations> cache = null;

    public VolumeIdCache(int maxEntries) {
        if (maxEntries == 0) {
            return;
        }
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maxEntries)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
    }

    public FilerProto.Locations getLocations(String volumeId) {
        if (this.cache == null) {
            return null;
        }
        return this.cache.getIfPresent(volumeId);
    }

    public void clearLocations(String volumeId) {
        if (this.cache == null) {
            return;
        }
        this.cache.invalidate(volumeId);
    }

    public void setLocations(String volumeId, FilerProto.Locations locations) {
        if (this.cache == null) {
            return;
        }
        this.cache.put(volumeId, locations);
    }

}
