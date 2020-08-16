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

public interface BlockStore {
    /**
     * Opens this store, calling the given action if the store is empty.
     */
    void open(Runnable initAction, Factory factory);

    /**
     * Closes this store.
     */
    void close();

    /**
     * Discards all blocks from this store.
     */
    void clear();

    /**
     * Removes the given block from this store.
     */
    void remove(BlockPayload block);

    /**
     * Reads the first block from this store.
     */
    <T extends BlockPayload> T readFirst(Class<T> payloadType);
    
    /**
     * Reads a block from this store.
     */
    <T extends BlockPayload> T read(BlockPointer pos, Class<T> payloadType);

    /**
     * Writes a block to this store, adding the block if required.
     */
    void write(BlockPayload block);

    /**
     * Adds a new block to this store. Allocates space for the block, but does not write the contents of the block
     * until {@link #write(BlockPayload)} is called.
     */
    void attach(BlockPayload block);

    /**
     * Flushes any pending updates for this store.
     */
    void flush();

    interface Factory {
        Object create(Class<? extends BlockPayload> type);
    }
}
