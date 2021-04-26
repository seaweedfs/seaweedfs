/*
 * Copyright 2010 the original author or authors.
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

import com.google.common.collect.ImmutableSet;
import seaweedfs.client.btree.serialize.Serializer;
import seaweedfs.client.btree.serialize.kryo.KryoBackedDecoder;
import seaweedfs.client.btree.serialize.kryo.KryoBackedEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// todo - stream serialised value to file
// todo - handle hash collisions (properly, this time)
// todo - don't store null links to child blocks in leaf index blocks
// todo - align block boundaries
// todo - thread safety control
// todo - merge small values into a single data block
// todo - discard when file corrupt
// todo - include data directly in index entry when serializer can guarantee small fixed sized data
// todo - free list leaks disk space
// todo - merge adjacent free blocks
// todo - use more efficient lookup for free block with nearest size
@SuppressWarnings("unchecked")
public class BTreePersistentIndexedCache<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BTreePersistentIndexedCache.class);
    private final File cacheFile;
    private final KeyHasher<K> keyHasher;
    private final Serializer<V> serializer;
    private final short maxChildIndexEntries;
    private final int minIndexChildNodes;
    private final StateCheckBlockStore store;
    private HeaderBlock header;

    public BTreePersistentIndexedCache(File cacheFile, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(cacheFile, keySerializer, valueSerializer, (short) 512, 512);
    }

    public BTreePersistentIndexedCache(File cacheFile, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                       short maxChildIndexEntries, int maxFreeListEntries) {
        this.cacheFile = cacheFile;
        this.keyHasher = new KeyHasher<K>(keySerializer);
        this.serializer = valueSerializer;
        this.maxChildIndexEntries = maxChildIndexEntries;
        this.minIndexChildNodes = maxChildIndexEntries / 2;
        BlockStore cachingStore = new CachingBlockStore(new FileBackedBlockStore(cacheFile), ImmutableSet.of(IndexBlock.class, FreeListBlockStore.FreeListBlock.class));
        this.store = new StateCheckBlockStore(new FreeListBlockStore(cachingStore, maxFreeListEntries));
        try {
            open();
        } catch (Exception e) {
            throw new UncheckedIOException(String.format("Could not open %s.", this), e);
        }
    }

    @Override
    public String toString() {
        return "cache " + cacheFile.getName() + " (" + cacheFile + ")";
    }

    private void open() throws Exception {
        LOGGER.debug("Opening {}", this);
        try {
            doOpen();
        } catch (CorruptedCacheException e) {
            rebuild();
        }
    }

    private void doOpen() throws Exception {
        BlockStore.Factory factory = new BlockStore.Factory() {
            @Override
            public Object create(Class<? extends BlockPayload> type) {
                if (type == HeaderBlock.class) {
                    return new HeaderBlock();
                }
                if (type == IndexBlock.class) {
                    return new IndexBlock();
                }
                if (type == DataBlock.class) {
                    return new DataBlock();
                }
                throw new UnsupportedOperationException();
            }
        };
        Runnable initAction = new Runnable() {
            @Override
            public void run() {
                header = new HeaderBlock();
                store.write(header);
                header.index.newRoot();
                store.flush();
            }
        };

        store.open(initAction, factory);
        header = store.readFirst(HeaderBlock.class);
    }

    public V get(K key) {
        try {
            try {
                DataBlock block = header.getRoot().get(key);
                if (block != null) {
                    return block.getValue();
                }
                return null;
            } catch (CorruptedCacheException e) {
                rebuild();
                return null;
            }
        } catch (Exception e) {
            throw new UncheckedIOException(String.format("Could not read entry '%s' from %s.", key, this), e);
        }
    }

    public void put(K key, V value) {
        try {
            long hashCode = keyHasher.getHashCode(key);
            Lookup lookup = header.getRoot().find(hashCode);
            DataBlock newBlock = null;
            if (lookup.entry != null) {
                DataBlock block = store.read(lookup.entry.dataBlock, DataBlock.class);
                DataBlockUpdateResult updateResult = block.useNewValue(value);
                if (updateResult.isFailed()) {
                    store.remove(block);
                    newBlock = new DataBlock(value, updateResult.getSerializedValue());
                }
            } else {
                newBlock = new DataBlock(value);
            }
            if (newBlock != null) {
                store.write(newBlock);
                lookup.indexBlock.put(hashCode, newBlock.getPos());
            }
            store.flush();
        } catch (Exception e) {
            throw new UncheckedIOException(String.format("Could not add entry '%s' to %s.", key, this), e);
        }
    }

    public void remove(K key) {
        try {
            Lookup lookup = header.getRoot().find(key);
            if (lookup.entry == null) {
                return;
            }
            lookup.indexBlock.remove(lookup.entry);
            DataBlock block = store.read(lookup.entry.dataBlock, DataBlock.class);
            store.remove(block);
            store.flush();
        } catch (Exception e) {
            throw new UncheckedIOException(String.format("Could not remove entry '%s' from %s.", key, this), e);
        }
    }

    private IndexBlock load(BlockPointer pos, IndexRoot root, IndexBlock parent, int index) {
        IndexBlock block = store.read(pos, IndexBlock.class);
        block.root = root;
        block.parent = parent;
        block.parentEntryIndex = index;
        return block;
    }

    public void reset() {
        close();
        try {
            open();
        } catch (Exception e) {
            throw new UncheckedIOException(e);
        }
    }

    public void close() {
        LOGGER.debug("Closing {}", this);
        try {
            store.close();
        } catch (Exception e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean isOpen() {
        return store.isOpen();
    }

    private void rebuild() {
        LOGGER.warn("{} is corrupt. Discarding.", this);
        try {
            clear();
        } catch (Exception e) {
            LOGGER.warn("{} couldn't be rebuilt. Closing.", this);
            close();
        }
    }

    public void verify() {
        try {
            doVerify();
        } catch (Exception e) {
            throw new UncheckedIOException(String.format("Some problems were found when checking the integrity of %s.",
                    this), e);
        }
    }

    private void doVerify() throws Exception {
        List<BlockPayload> blocks = new ArrayList<BlockPayload>();

        HeaderBlock header = store.readFirst(HeaderBlock.class);
        blocks.add(header);
        verifyTree(header.getRoot(), "", blocks, Long.MAX_VALUE, true);

        Collections.sort(blocks, new Comparator<BlockPayload>() {
            @Override
            public int compare(BlockPayload block, BlockPayload block1) {
                return block.getPos().compareTo(block1.getPos());
            }
        });

        for (int i = 0; i < blocks.size() - 1; i++) {
            Block b1 = blocks.get(i).getBlock();
            Block b2 = blocks.get(i + 1).getBlock();
            if (b1.getPos().getPos() + b1.getSize() > b2.getPos().getPos()) {
                throw new IOException(String.format("%s overlaps with %s", b1, b2));
            }
        }
    }

    private void verifyTree(IndexBlock current, String prefix, Collection<BlockPayload> blocks, long maxValue,
                            boolean loadData) throws Exception {
        blocks.add(current);

        if (!prefix.equals("") && current.entries.size() < maxChildIndexEntries / 2) {
            throw new IOException(String.format("Too few entries found in %s", current));
        }
        if (current.entries.size() > maxChildIndexEntries) {
            throw new IOException(String.format("Too many entries found in %s", current));
        }

        boolean isLeaf = current.entries.size() == 0 || current.entries.get(0).childIndexBlock.isNull();
        if (isLeaf ^ current.tailPos.isNull()) {
            throw new IOException(String.format("Mismatched leaf/tail-node in %s", current));
        }

        long min = Long.MIN_VALUE;
        for (IndexEntry entry : current.entries) {
            if (isLeaf ^ entry.childIndexBlock.isNull()) {
                throw new IOException(String.format("Mismatched leaf/non-leaf entry in %s", current));
            }
            if (entry.hashCode >= maxValue || entry.hashCode <= min) {
                throw new IOException(String.format("Out-of-order key in %s", current));
            }
            min = entry.hashCode;
            if (!entry.childIndexBlock.isNull()) {
                IndexBlock child = store.read(entry.childIndexBlock, IndexBlock.class);
                verifyTree(child, "   " + prefix, blocks, entry.hashCode, loadData);
            }
            if (loadData) {
                DataBlock block = store.read(entry.dataBlock, DataBlock.class);
                blocks.add(block);
            }
        }
        if (!current.tailPos.isNull()) {
            IndexBlock tail = store.read(current.tailPos, IndexBlock.class);
            verifyTree(tail, "   " + prefix, blocks, maxValue, loadData);
        }
    }

    public void clear() {
        store.clear();
        close();
        try {
            doOpen();
        } catch (Exception e) {
            throw new UncheckedIOException(e);
        }
    }

    private class IndexRoot {
        private BlockPointer rootPos = BlockPointer.start();
        private HeaderBlock owner;

        private IndexRoot(HeaderBlock owner) {
            this.owner = owner;
        }

        public void setRootPos(BlockPointer rootPos) {
            this.rootPos = rootPos;
            store.write(owner);
        }

        public IndexBlock getRoot() {
            return load(rootPos, this, null, 0);
        }

        public IndexBlock newRoot() {
            IndexBlock block = new IndexBlock();
            store.write(block);
            setRootPos(block.getPos());
            return block;
        }
    }

    private class HeaderBlock extends BlockPayload {
        private IndexRoot index;

        private HeaderBlock() {
            index = new IndexRoot(this);
        }

        @Override
        protected byte getType() {
            return 0x55;
        }

        @Override
        protected int getSize() {
            return Block.LONG_SIZE + Block.SHORT_SIZE;
        }

        @Override
        protected void read(DataInputStream instr) throws Exception {
            index.rootPos = BlockPointer.pos(instr.readLong());

            short actualChildIndexEntries = instr.readShort();
            if (actualChildIndexEntries != maxChildIndexEntries) {
                throw blockCorruptedException();
            }
        }

        @Override
        protected void write(DataOutputStream outstr) throws Exception {
            outstr.writeLong(index.rootPos.getPos());
            outstr.writeShort(maxChildIndexEntries);
        }

        public IndexBlock getRoot() throws Exception {
            return index.getRoot();
        }
    }

    private class IndexBlock extends BlockPayload {
        private final List<IndexEntry> entries = new ArrayList<IndexEntry>();
        private BlockPointer tailPos = BlockPointer.start();
        // Transient fields
        private IndexBlock parent;
        private int parentEntryIndex;
        private IndexRoot root;

        @Override
        protected byte getType() {
            return 0x77;
        }

        @Override
        protected int getSize() {
            return Block.INT_SIZE + Block.LONG_SIZE + (3 * Block.LONG_SIZE) * maxChildIndexEntries;
        }

        @Override
        public void read(DataInputStream instr) throws IOException {
            int count = instr.readInt();
            entries.clear();
            for (int i = 0; i < count; i++) {
                IndexEntry entry = new IndexEntry();
                entry.hashCode = instr.readLong();
                entry.dataBlock = BlockPointer.pos(instr.readLong());
                entry.childIndexBlock = BlockPointer.pos(instr.readLong());
                entries.add(entry);
            }
            tailPos = BlockPointer.pos(instr.readLong());
        }

        @Override
        public void write(DataOutputStream outstr) throws IOException {
            outstr.writeInt(entries.size());
            for (IndexEntry entry : entries) {
                outstr.writeLong(entry.hashCode);
                outstr.writeLong(entry.dataBlock.getPos());
                outstr.writeLong(entry.childIndexBlock.getPos());
            }
            outstr.writeLong(tailPos.getPos());
        }

        public void put(long hashCode, BlockPointer pos) throws Exception {
            int index = Collections.binarySearch(entries, new IndexEntry(hashCode));
            IndexEntry entry;
            if (index >= 0) {
                entry = entries.get(index);
            } else {
                assert tailPos.isNull();
                entry = new IndexEntry();
                entry.hashCode = hashCode;
                entry.childIndexBlock = BlockPointer.start();
                index = -index - 1;
                entries.add(index, entry);
            }

            entry.dataBlock = pos;
            store.write(this);

            maybeSplit();
        }

        private void maybeSplit() throws Exception {
            if (entries.size() > maxChildIndexEntries) {
                int splitPos = entries.size() / 2;
                IndexEntry splitEntry = entries.remove(splitPos);
                if (parent == null) {
                    parent = root.newRoot();
                }
                IndexBlock sibling = new IndexBlock();
                store.write(sibling);
                List<IndexEntry> siblingEntries = entries.subList(splitPos, entries.size());
                sibling.entries.addAll(siblingEntries);
                siblingEntries.clear();
                sibling.tailPos = tailPos;
                tailPos = splitEntry.childIndexBlock;
                splitEntry.childIndexBlock = BlockPointer.start();
                parent.add(this, splitEntry, sibling);
            }
        }

        private void add(IndexBlock left, IndexEntry entry, IndexBlock right) throws Exception {
            int index = left.parentEntryIndex;
            if (index < entries.size()) {
                IndexEntry parentEntry = entries.get(index);
                assert parentEntry.childIndexBlock.equals(left.getPos());
                parentEntry.childIndexBlock = right.getPos();
            } else {
                assert index == entries.size() && (tailPos.isNull() || tailPos.equals(left.getPos()));
                tailPos = right.getPos();
            }
            entries.add(index, entry);
            entry.childIndexBlock = left.getPos();
            store.write(this);

            maybeSplit();
        }

        public DataBlock get(K key) throws Exception {
            Lookup lookup = find(key);
            if (lookup.entry == null) {
                return null;
            }

            return store.read(lookup.entry.dataBlock, DataBlock.class);
        }

        public Lookup find(K key) throws Exception {
            long checksum = keyHasher.getHashCode(key);
            return find(checksum);
        }

        private Lookup find(long hashCode) throws Exception {
            int index = Collections.binarySearch(entries, new IndexEntry(hashCode));
            if (index >= 0) {
                return new Lookup(this, entries.get(index));
            }

            index = -index - 1;
            BlockPointer childBlockPos;
            if (index == entries.size()) {
                childBlockPos = tailPos;
            } else {
                childBlockPos = entries.get(index).childIndexBlock;
            }
            if (childBlockPos.isNull()) {
                return new Lookup(this, null);
            }

            IndexBlock childBlock = load(childBlockPos, root, this, index);
            return childBlock.find(hashCode);
        }

        public void remove(IndexEntry entry) throws Exception {
            int index = entries.indexOf(entry);
            assert index >= 0;
            entries.remove(index);
            store.write(this);

            if (entry.childIndexBlock.isNull()) {
                maybeMerge();
            } else {
                // Not a leaf node. Move up an entry from a leaf node, then possibly merge the leaf node
                IndexBlock leafBlock = load(entry.childIndexBlock, root, this, index);
                leafBlock = leafBlock.findHighestLeaf();
                IndexEntry highestEntry = leafBlock.entries.remove(leafBlock.entries.size() - 1);
                highestEntry.childIndexBlock = entry.childIndexBlock;
                entries.add(index, highestEntry);
                store.write(leafBlock);
                leafBlock.maybeMerge();
            }
        }

        private void maybeMerge() throws Exception {
            if (parent == null) {
                // This is the root block. Can have any number of children <= maxChildIndexEntries
                if (entries.size() == 0 && !tailPos.isNull()) {
                    // This is an empty root block, discard it
                    header.index.setRootPos(tailPos);
                    store.remove(this);
                }
                return;
            }

            // This is not the root block. Must have children >= minIndexChildNodes
            if (entries.size() >= minIndexChildNodes) {
                return;
            }

            // Attempt to merge with the left sibling
            IndexBlock left = parent.getPrevious(this);
            if (left != null) {
                assert entries.size() + left.entries.size() <= maxChildIndexEntries * 2;
                if (left.entries.size() > minIndexChildNodes) {
                    // There are enough entries in this block and the left sibling to make up 2 blocks, so redistribute
                    // the entries evenly between them
                    left.mergeFrom(this);
                    left.maybeSplit();
                    return;
                } else {
                    // There are only enough entries to make up 1 block, so move the entries of the left sibling into
                    // this block and discard the left sibling. Might also need to merge the parent
                    left.mergeFrom(this);
                    parent.maybeMerge();
                    return;
                }
            }

            // Attempt to merge with the right sibling
            IndexBlock right = parent.getNext(this);
            if (right != null) {
                assert entries.size() + right.entries.size() <= maxChildIndexEntries * 2;
                if (right.entries.size() > minIndexChildNodes) {
                    // There are enough entries in this block and the right sibling to make up 2 blocks, so redistribute
                    // the entries evenly between them
                    mergeFrom(right);
                    maybeSplit();
                    return;
                } else {
                    // There are only enough entries to make up 1 block, so move the entries of the right sibling into
                    // this block and discard this block. Might also need to merge the parent
                    mergeFrom(right);
                    parent.maybeMerge();
                    return;
                }
            }

            // Should not happen
            throw new IllegalStateException(String.format("%s does not have any siblings.", getBlock()));
        }

        private void mergeFrom(IndexBlock right) throws Exception {
            IndexEntry newChildEntry = parent.entries.remove(parentEntryIndex);
            if (right.getPos().equals(parent.tailPos)) {
                parent.tailPos = getPos();
            } else {
                IndexEntry newParentEntry = parent.entries.get(parentEntryIndex);
                assert newParentEntry.childIndexBlock.equals(right.getPos());
                newParentEntry.childIndexBlock = getPos();
            }
            entries.add(newChildEntry);
            entries.addAll(right.entries);
            newChildEntry.childIndexBlock = tailPos;
            tailPos = right.tailPos;
            store.write(parent);
            store.write(this);
            store.remove(right);
        }

        private IndexBlock getNext(IndexBlock indexBlock) throws Exception {
            int index = indexBlock.parentEntryIndex + 1;
            if (index > entries.size()) {
                return null;
            }
            if (index == entries.size()) {
                return load(tailPos, root, this, index);
            }
            return load(entries.get(index).childIndexBlock, root, this, index);
        }

        private IndexBlock getPrevious(IndexBlock indexBlock) throws Exception {
            int index = indexBlock.parentEntryIndex - 1;
            if (index < 0) {
                return null;
            }
            return load(entries.get(index).childIndexBlock, root, this, index);
        }

        private IndexBlock findHighestLeaf() throws Exception {
            if (tailPos.isNull()) {
                return this;
            }
            return load(tailPos, root, this, entries.size()).findHighestLeaf();
        }
    }

    private static class IndexEntry implements Comparable<IndexEntry> {
        long hashCode;
        BlockPointer dataBlock;
        BlockPointer childIndexBlock;

        private IndexEntry() {
        }

        private IndexEntry(long hashCode) {
            this.hashCode = hashCode;
        }

        @Override
        public int compareTo(IndexEntry indexEntry) {
            if (hashCode > indexEntry.hashCode) {
                return 1;
            }
            if (hashCode < indexEntry.hashCode) {
                return -1;
            }
            return 0;
        }
    }

    private class Lookup {
        final IndexBlock indexBlock;
        final IndexEntry entry;

        private Lookup(IndexBlock indexBlock, IndexEntry entry) {
            this.indexBlock = indexBlock;
            this.entry = entry;
        }
    }

    private class DataBlock extends BlockPayload {
        private int size;
        private StreamByteBuffer buffer;
        private V value;

        private DataBlock() {
        }

        public DataBlock(V value) throws Exception {
            this.value = value;
            setValue(value);
            size = buffer.totalBytesUnread();
        }

        public DataBlock(V value, StreamByteBuffer buffer) throws Exception {
            this.value = value;
            this.buffer = buffer;
            size = buffer.totalBytesUnread();
        }

        public void setValue(V value) throws Exception {
            buffer = StreamByteBuffer.createWithChunkSizeInDefaultRange(size);
            KryoBackedEncoder encoder = new KryoBackedEncoder(buffer.getOutputStream());
            serializer.write(encoder, value);
            encoder.flush();
        }

        public V getValue() throws Exception {
            if (value == null) {
                value = serializer.read(new KryoBackedDecoder(buffer.getInputStream()));
                buffer = null;
            }
            return value;
        }

        @Override
        protected byte getType() {
            return 0x33;
        }

        @Override
        protected int getSize() {
            return 2 * Block.INT_SIZE + size;
        }

        @Override
        public void read(DataInputStream instr) throws Exception {
            size = instr.readInt();
            int bytes = instr.readInt();
            buffer = StreamByteBuffer.of(instr, bytes);
        }

        @Override
        public void write(DataOutputStream outstr) throws Exception {
            outstr.writeInt(size);
            outstr.writeInt(buffer.totalBytesUnread());
            buffer.writeTo(outstr);
            buffer = null;
        }

        public DataBlockUpdateResult useNewValue(V value) throws Exception {
            setValue(value);
            boolean ok = buffer.totalBytesUnread() <= size;
            if (ok) {
                this.value = value;
                store.write(this);
                return DataBlockUpdateResult.success();
            } else {
                return DataBlockUpdateResult.failed(buffer);
            }
        }
    }

    private static class DataBlockUpdateResult {
        private static final DataBlockUpdateResult SUCCESS = new DataBlockUpdateResult(true, null);
        private final boolean success;
        private final StreamByteBuffer serializedValue;

        private DataBlockUpdateResult(boolean success, StreamByteBuffer serializedValue) {
            this.success = success;
            this.serializedValue = serializedValue;
        }

        static DataBlockUpdateResult success() {
            return SUCCESS;
        }

        static DataBlockUpdateResult failed(StreamByteBuffer serializedValue) {
            return new DataBlockUpdateResult(false, serializedValue);
        }

        public boolean isFailed() {
            return !success;
        }

        public StreamByteBuffer getSerializedValue() {
            return serializedValue;
        }
    }
}
