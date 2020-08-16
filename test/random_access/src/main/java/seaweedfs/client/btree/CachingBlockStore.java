/*
 * Copyright 2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package seaweedfs.client.btree;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class CachingBlockStore implements BlockStore {
    private final BlockStore store;
    private final Map<BlockPointer, BlockPayload> dirty = new LinkedHashMap<BlockPointer, BlockPayload>();
    private final Cache<BlockPointer, BlockPayload> indexBlockCache = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(1).build();
    private final ImmutableSet<Class<? extends BlockPayload>> cacheableBlockTypes;

    public CachingBlockStore(BlockStore store, Collection<Class<? extends BlockPayload>> cacheableBlockTypes) {
        this.store = store;
        this.cacheableBlockTypes = ImmutableSet.copyOf(cacheableBlockTypes);
    }

    @Override
    public void open(Runnable initAction, Factory factory) {
        store.open(initAction, factory);
    }

    @Override
    public void close() {
        flush();
        indexBlockCache.invalidateAll();
        store.close();
    }

    @Override
    public void clear() {
        dirty.clear();
        indexBlockCache.invalidateAll();
        store.clear();
    }

    @Override
    public void flush() {
        Iterator<BlockPayload> iterator = dirty.values().iterator();
        while (iterator.hasNext()) {
            BlockPayload block = iterator.next();
            iterator.remove();
            store.write(block);
        }
        store.flush();
    }

    @Override
    public void attach(BlockPayload block) {
        store.attach(block);
    }

    @Override
    public void remove(BlockPayload block) {
        dirty.remove(block.getPos());
        if (isCacheable(block)) {
            indexBlockCache.invalidate(block.getPos());
        }
        store.remove(block);
    }

    @Override
    public <T extends BlockPayload> T readFirst(Class<T> payloadType) {
        T block = store.readFirst(payloadType);
        maybeCache(block);
        return block;
    }

    @Override
    public <T extends BlockPayload> T read(BlockPointer pos, Class<T> payloadType) {
        T block = payloadType.cast(dirty.get(pos));
        if (block != null) {
            return block;
        }
        block = maybeGetFromCache(pos, payloadType);
        if (block != null) {
            return block;
        }
        block = store.read(pos, payloadType);
        maybeCache(block);
        return block;
    }

    @Nullable
    private <T extends BlockPayload> T maybeGetFromCache(BlockPointer pos, Class<T> payloadType) {
        if (cacheableBlockTypes.contains(payloadType)) {
            return payloadType.cast(indexBlockCache.getIfPresent(pos));
        }
        return null;
    }

    @Override
    public void write(BlockPayload block) {
        store.attach(block);
        maybeCache(block);
        dirty.put(block.getPos(), block);
    }

    private <T extends BlockPayload> void maybeCache(T block) {
        if (isCacheable(block)) {
            indexBlockCache.put(block.getPos(), block);
        }
    }

    private <T extends BlockPayload> boolean isCacheable(T block) {
        return cacheableBlockTypes.contains(block.getClass());
    }
}
