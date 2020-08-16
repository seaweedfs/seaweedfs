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

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Maps;
import seaweedfs.client.btree.serialize.AbstractEncoder;
import seaweedfs.client.btree.serialize.FlushableEncoder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.OutputStream;
import java.util.Map;

public class StringDeduplicatingKryoBackedEncoder extends AbstractEncoder implements FlushableEncoder, Closeable {
    private Map<String, Integer> strings;

    private final Output output;

    public StringDeduplicatingKryoBackedEncoder(OutputStream outputStream) {
        this(outputStream, 4096);
    }

    public StringDeduplicatingKryoBackedEncoder(OutputStream outputStream, int bufferSize) {
        output = new Output(outputStream, bufferSize);
    }

    @Override
    public void writeByte(byte value) {
        output.writeByte(value);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int count) {
        output.writeBytes(bytes, offset, count);
    }

    @Override
    public void writeLong(long value) {
        output.writeLong(value);
    }

    @Override
    public void writeSmallLong(long value) {
        output.writeLong(value, true);
    }

    @Override
    public void writeInt(int value) {
        output.writeInt(value);
    }

    @Override
    public void writeSmallInt(int value) {
        output.writeInt(value, true);
    }

    @Override
    public void writeBoolean(boolean value) {
        output.writeBoolean(value);
    }

    @Override
    public void writeString(CharSequence value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot encode a null string.");
        }
        writeNullableString(value);
    }

    @Override
    public void writeNullableString(@Nullable CharSequence value) {
        if (value == null) {
            output.writeInt(-1);
            return;
        } else {
            if (strings == null) {
                strings = Maps.newHashMapWithExpectedSize(1024);
            }
        }
        String key = value.toString();
        Integer index = strings.get(key);
        if (index == null) {
            index = strings.size();
            output.writeInt(index);
            strings.put(key, index);
            output.writeString(key);
        } else {
            output.writeInt(index);
        }
    }

    /**
     * Returns the total number of bytes written by this encoder, some of which may still be buffered.
     */
    public long getWritePosition() {
        return output.total();
    }

    @Override
    public void flush() {
        output.flush();
    }

    @Override
    public void close() {
        output.close();
    }

    public void done() {
        strings = null;
    }

}
