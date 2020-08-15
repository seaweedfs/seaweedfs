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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FreeListBlockStore implements BlockStore {
    private final BlockStore store;
    private final BlockStore freeListStore;
    private final int maxBlockEntries;
    private FreeListBlock freeListBlock;

    public FreeListBlockStore(BlockStore store, int maxBlockEntries) {
        this.store = store;
        freeListStore = this;
        this.maxBlockEntries = maxBlockEntries;
    }

    @Override
    public void open(final Runnable initAction, final Factory factory) {
        Runnable freeListInitAction = new Runnable() {
            @Override
            public void run() {
                freeListBlock = new FreeListBlock();
                store.write(freeListBlock);
                store.flush();
                initAction.run();
            }
        };
        Factory freeListFactory = new Factory() {
            @Override
            public Object create(Class<? extends BlockPayload> type) {
                if (type == FreeListBlock.class) {
                    return new FreeListBlock();
                }
                return factory.create(type);
            }
        };

        store.open(freeListInitAction, freeListFactory);
        freeListBlock = store.readFirst(FreeListBlock.class);
    }

    @Override
    public void close() {
        freeListBlock = null;
        store.close();
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void remove(BlockPayload block) {
        Block container = block.getBlock();
        store.remove(block);
        freeListBlock.add(container.getPos(), container.getSize());
    }

    @Override
    public <T extends BlockPayload> T readFirst(Class<T> payloadType) {
        return store.read(freeListBlock.getNextPos(), payloadType);
    }

    @Override
    public <T extends BlockPayload> T read(BlockPointer pos, Class<T> payloadType) {
        return store.read(pos, payloadType);
    }

    @Override
    public void write(BlockPayload block) {
        attach(block);
        store.write(block);
    }

    @Override
    public void attach(BlockPayload block) {
        store.attach(block);
        freeListBlock.alloc(block.getBlock());
    }

    @Override
    public void flush() {
        store.flush();
    }

    private void verify() {
        FreeListBlock block = store.readFirst(FreeListBlock.class);
        verify(block, Integer.MAX_VALUE);
    }

    private void verify(FreeListBlock block, int maxValue) {
        if (block.largestInNextBlock > maxValue) {
            throw new RuntimeException("corrupt free list");
        }
        int current = 0;
        for (FreeListEntry entry : block.entries) {
            if (entry.size > maxValue) {
                throw new RuntimeException("corrupt free list");
            }
            if (entry.size < block.largestInNextBlock) {
                throw new RuntimeException("corrupt free list");
            }
            if (entry.size < current) {
                throw new RuntimeException("corrupt free list");
            }
            current = entry.size;
        }
        if (!block.nextBlock.isNull()) {
            verify(store.read(block.nextBlock, FreeListBlock.class), block.largestInNextBlock);
        }
    }

    public class FreeListBlock extends BlockPayload {
        private List<FreeListEntry> entries = new ArrayList<FreeListEntry>();
        private int largestInNextBlock;
        private BlockPointer nextBlock = BlockPointer.start();
        // Transient fields
        private FreeListBlock prev;
        private FreeListBlock next;

        @Override
        protected int getSize() {
            return Block.LONG_SIZE + Block.INT_SIZE + Block.INT_SIZE + maxBlockEntries * (Block.LONG_SIZE
                    + Block.INT_SIZE);
        }

        @Override
        protected byte getType() {
            return 0x44;
        }

        @Override
        protected void read(DataInputStream inputStream) throws Exception {
            nextBlock = BlockPointer.pos(inputStream.readLong());
            largestInNextBlock = inputStream.readInt();
            int count = inputStream.readInt();
            for (int i = 0; i < count; i++) {
                BlockPointer pos = BlockPointer.pos(inputStream.readLong());
                int size = inputStream.readInt();
                entries.add(new FreeListEntry(pos, size));
            }
        }

        @Override
        protected void write(DataOutputStream outputStream) throws Exception {
            outputStream.writeLong(nextBlock.getPos());
            outputStream.writeInt(largestInNextBlock);
            outputStream.writeInt(entries.size());
            for (FreeListEntry entry : entries) {
                outputStream.writeLong(entry.pos.getPos());
                outputStream.writeInt(entry.size);
            }
        }

        public void add(BlockPointer pos, int size) {
            assert !pos.isNull() && size >= 0;
            if (size == 0) {
                return;
            }

            if (size < largestInNextBlock) {
                FreeListBlock next = getNextBlock();
                next.add(pos, size);
                return;
            }

            FreeListEntry entry = new FreeListEntry(pos, size);
            int index = Collections.binarySearch(entries, entry);
            if (index < 0) {
                index = -index - 1;
            }
            entries.add(index, entry);

            if (entries.size() > maxBlockEntries) {
                FreeListBlock newBlock = new FreeListBlock();
                newBlock.largestInNextBlock = largestInNextBlock;
                newBlock.nextBlock = nextBlock;
                newBlock.prev = this;
                newBlock.next = next;
                next = newBlock;

                List<FreeListEntry> newBlockEntries = entries.subList(0, entries.size() / 2);
                newBlock.entries.addAll(newBlockEntries);
                newBlockEntries.clear();
                largestInNextBlock = newBlock.entries.get(newBlock.entries.size() - 1).size;
                freeListStore.write(newBlock);
                nextBlock = newBlock.getPos();
            }

            freeListStore.write(this);
        }

        private FreeListBlock getNextBlock() {
            if (next == null) {
                next = freeListStore.read(nextBlock, FreeListBlock.class);
                next.prev = this;
            }
            return next;
        }

        public void alloc(Block block) {
            if (block.hasPos()) {
                return;
            }

            int requiredSize = block.getSize();

            if (entries.isEmpty() || requiredSize <= largestInNextBlock) {
                if (nextBlock.isNull()) {
                    return;
                }
                getNextBlock().alloc(block);
                return;
            }

            int index = Collections.binarySearch(entries, new FreeListEntry(null, requiredSize));
            if (index < 0) {
                index = -index - 1;
            }
            if (index == entries.size()) {
                // Largest free block is too small
                return;
            }

            FreeListEntry entry = entries.remove(index);
            block.setPos(entry.pos);
            block.setSize(entry.size);
            freeListStore.write(this);

            if (entries.size() == 0 && prev != null) {
                prev.nextBlock = nextBlock;
                prev.largestInNextBlock = largestInNextBlock;
                prev.next = next;
                if (next != null) {
                    next.prev = prev;
                }
                freeListStore.write(prev);
                freeListStore.remove(this);
            }
        }
    }

    private static class FreeListEntry implements Comparable<FreeListEntry> {
        final BlockPointer pos;
        final int size;

        private FreeListEntry(BlockPointer pos, int size) {
            this.pos = pos;
            this.size = size;
        }

        @Override
        public int compareTo(FreeListEntry o) {
            if (size > o.size) {
                return 1;
            }
            if (size < o.size) {
                return -1;
            }
            return 0;
        }
    }
}
