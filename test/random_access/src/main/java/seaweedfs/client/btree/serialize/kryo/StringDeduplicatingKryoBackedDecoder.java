/*
 * Copyright 2018 the original author or authors.
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

package seaweedfs.client.btree.serialize.kryo;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import seaweedfs.client.btree.serialize.AbstractDecoder;
import seaweedfs.client.btree.serialize.Decoder;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Note that this decoder uses buffering, so will attempt to read beyond the end of the encoded data. This means you should use this type only when this decoder will be used to decode the entire
 * stream.
 */
public class StringDeduplicatingKryoBackedDecoder extends AbstractDecoder implements Decoder, Closeable {
    public static final int INITIAL_CAPACITY = 32;
    private final Input input;
    private final InputStream inputStream;
    private String[] strings;
    private long extraSkipped;

    public StringDeduplicatingKryoBackedDecoder(InputStream inputStream) {
        this(inputStream, 4096);
    }

    public StringDeduplicatingKryoBackedDecoder(InputStream inputStream, int bufferSize) {
        this.inputStream = inputStream;
        input = new Input(this.inputStream, bufferSize);
    }

    @Override
    protected int maybeReadBytes(byte[] buffer, int offset, int count) {
        return input.read(buffer, offset, count);
    }

    @Override
    protected long maybeSkip(long count) throws IOException {
        // Work around some bugs in Input.skip()
        int remaining = input.limit() - input.position();
        if (remaining == 0) {
            long skipped = inputStream.skip(count);
            if (skipped > 0) {
                extraSkipped += skipped;
            }
            return skipped;
        } else if (count <= remaining) {
            input.setPosition(input.position() + (int) count);
            return count;
        } else {
            input.setPosition(input.limit());
            return remaining;
        }
    }

    private RuntimeException maybeEndOfStream(KryoException e) throws EOFException {
        if (e.getMessage().equals("Buffer underflow.")) {
            throw (EOFException) (new EOFException().initCause(e));
        }
        throw e;
    }

    @Override
    public byte readByte() throws EOFException {
        try {
            return input.readByte();
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public void readBytes(byte[] buffer, int offset, int count) throws EOFException {
        try {
            input.readBytes(buffer, offset, count);
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public long readLong() throws EOFException {
        try {
            return input.readLong();
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public long readSmallLong() throws EOFException, IOException {
        try {
            return input.readLong(true);
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public int readInt() throws EOFException {
        try {
            return input.readInt();
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public int readSmallInt() throws EOFException {
        try {
            return input.readInt(true);
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public boolean readBoolean() throws EOFException {
        try {
            return input.readBoolean();
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    @Override
    public String readString() throws EOFException {
        return readNullableString();
    }

    @Override
    public String readNullableString() throws EOFException {
        try {
            int idx = readInt();
            if (idx == -1) {
                return null;
            }
            if (strings == null) {
                strings = new String[INITIAL_CAPACITY];
            }
            String string = null;
            if (idx >= strings.length) {
                String[] grow = new String[strings.length * 3 / 2];
                System.arraycopy(strings, 0, grow, 0, strings.length);
                strings = grow;
            } else {
                string = strings[idx];
            }
            if (string == null) {
                string = input.readString();
                strings[idx] = string;
            }
            return string;
        } catch (KryoException e) {
            throw maybeEndOfStream(e);
        }
    }

    /**
     * Returns the total number of bytes consumed by this decoder. Some additional bytes may also be buffered by this decoder but have not been consumed.
     */
    public long getReadPosition() {
        return input.total() + extraSkipped;
    }

    @Override
    public void close() throws IOException {
        strings = null;
        input.close();
    }
}
