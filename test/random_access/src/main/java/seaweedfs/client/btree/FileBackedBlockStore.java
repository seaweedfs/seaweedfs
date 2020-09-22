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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileBackedBlockStore implements BlockStore {
    private final File cacheFile;
    private RandomAccessFile file;
    private ByteOutput output;
    private ByteInput input;
    private long nextBlock;
    private Factory factory;
    private long currentFileSize;

    public FileBackedBlockStore(File cacheFile) {
        this.cacheFile = cacheFile;
    }

    @Override
    public String toString() {
        return "cache '" + cacheFile + "'";
    }

    @Override
    public void open(Runnable runnable, Factory factory) {
        this.factory = factory;
        try {
            cacheFile.getParentFile().mkdirs();
            file = openRandomAccessFile();
            output = new ByteOutput(file);
            input = new ByteInput(file);
            currentFileSize = file.length();
            nextBlock = currentFileSize;
            if (currentFileSize == 0) {
                runnable.run();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RandomAccessFile openRandomAccessFile() throws FileNotFoundException {
        try {
            return randomAccessFile("rw");
        } catch (FileNotFoundException e) {
            return randomAccessFile("r");
        }
    }

    private RandomAccessFile randomAccessFile(String mode) throws FileNotFoundException {
        return new RandomAccessFile(cacheFile, mode);
    }

    @Override
    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void clear() {
        try {
            file.setLength(0);
            currentFileSize = 0;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        nextBlock = 0;
    }

    @Override
    public void attach(BlockPayload block) {
        if (block.getBlock() == null) {
            block.setBlock(new BlockImpl(block));
        }
    }

    @Override
    public void remove(BlockPayload block) {
        BlockImpl blockImpl = (BlockImpl) block.getBlock();
        blockImpl.detach();
    }

    @Override
    public void flush() {
    }

    @Override
    public <T extends BlockPayload> T readFirst(Class<T> payloadType) {
        return read(BlockPointer.pos(0), payloadType);
    }

    @Override
    public <T extends BlockPayload> T read(BlockPointer pos, Class<T> payloadType) {
        assert !pos.isNull();
        try {
            T payload = payloadType.cast(factory.create(payloadType));
            BlockImpl block = new BlockImpl(payload, pos);
            block.read();
            return payload;
        } catch (CorruptedCacheException e) {
            throw e;
        } catch (Exception e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void write(BlockPayload block) {
        BlockImpl blockImpl = (BlockImpl) block.getBlock();
        try {
            blockImpl.write();
        } catch (CorruptedCacheException e) {
            throw e;
        } catch (Exception e) {
            throw new UncheckedIOException(e);
        }
    }

    private long alloc(long length) {
        long pos = nextBlock;
        nextBlock += length;
        return pos;
    }

    private final class BlockImpl extends Block {
        private static final int HEADER_SIZE = 1 + INT_SIZE; // type, payload size
        private static final int TAIL_SIZE = INT_SIZE;

        private BlockPointer pos;
        private int payloadSize;

        private BlockImpl(BlockPayload payload, BlockPointer pos) {
            this(payload);
            setPos(pos);
        }

        public BlockImpl(BlockPayload payload) {
            super(payload);
            pos = null;
            payloadSize = -1;
        }

        @Override
        public boolean hasPos() {
            return pos != null;
        }

        @Override
        public BlockPointer getPos() {
            if (pos == null) {
                pos = BlockPointer.pos(alloc(getSize()));
            }
            return pos;
        }

        @Override
        public void setPos(BlockPointer pos) {
            assert this.pos == null && !pos.isNull();
            this.pos = pos;
        }

        @Override
        public int getSize() {
            if (payloadSize < 0) {
                payloadSize = getPayload().getSize();
            }
            return payloadSize + HEADER_SIZE + TAIL_SIZE;
        }

        @Override
        public void setSize(int size) {
            int newPayloadSize = size - HEADER_SIZE - TAIL_SIZE;
            assert newPayloadSize >= payloadSize;
            payloadSize = newPayloadSize;
        }

        public void write() throws Exception {
            long pos = getPos().getPos();

            DataOutputStream outputStream = output.start(pos);

            BlockPayload payload = getPayload();

            // Write header
            outputStream.writeByte(payload.getType());
            outputStream.writeInt(payloadSize);
            long finalSize = pos + HEADER_SIZE + TAIL_SIZE + payloadSize;

            // Write body
            payload.write(outputStream);

            // Write count
            long bytesWritten = output.getBytesWritten();
            if (bytesWritten > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Block payload exceeds maximum size");
            }
            outputStream.writeInt((int) bytesWritten);
            output.done();

            // System.out.println(String.format("wrote [%d,%d)", pos, pos + bytesWritten + 4));

            // Pad
            if (currentFileSize < finalSize) {
                // System.out.println(String.format("pad length %d => %d", currentFileSize, finalSize));
                file.setLength(finalSize);
                currentFileSize = finalSize;
            }
        }

        public void read() throws Exception {
            long pos = getPos().getPos();
            assert pos >= 0;
            if (pos + HEADER_SIZE >= currentFileSize) {
                throw blockCorruptedException();
            }

            DataInputStream inputStream = input.start(pos);

            BlockPayload payload = getPayload();

            // Read header
            byte type = inputStream.readByte();
            if (type != payload.getType()) {
                throw blockCorruptedException();
            }

            // Read body
            payloadSize = inputStream.readInt();
            if (pos + HEADER_SIZE + TAIL_SIZE + payloadSize > currentFileSize) {
                throw blockCorruptedException();
            }
            payload.read(inputStream);

            // Read and verify count
            long actualCount = input.getBytesRead();
            long count = inputStream.readInt();
            if (actualCount != count) {
                System.out.println(String.format("read expected %d actual %d, pos %d payloadSize %d currentFileSize %d", count, actualCount, pos, payloadSize, currentFileSize));
                throw blockCorruptedException();
            }
            input.done();
        }

        @Override
        public RuntimeException blockCorruptedException() {
            return new CorruptedCacheException(String.format("Corrupted %s found in %s.", this,
                    FileBackedBlockStore.this));
        }
    }

}
