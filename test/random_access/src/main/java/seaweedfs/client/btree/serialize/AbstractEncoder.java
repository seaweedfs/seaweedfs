/*
 * Copyright 2013 the original author or authors.
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

package seaweedfs.client.btree.serialize;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractEncoder implements Encoder {
    private EncoderStream stream;

    @Override
    public OutputStream getOutputStream() {
        if (stream == null) {
            stream = new EncoderStream();
        }
        return stream;
    }

    @Override
    public void writeBytes(byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {
        writeBinary(bytes, 0, bytes.length);
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int count) throws IOException {
        writeSmallInt(count);
        writeBytes(bytes, offset, count);
    }

    @Override
    public void encodeChunked(EncodeAction<Encoder> writeAction) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSmallInt(int value) throws IOException {
        writeInt(value);
    }

    @Override
    public void writeSmallLong(long value) throws IOException {
        writeLong(value);
    }

    @Override
    public void writeNullableSmallInt(@Nullable Integer value) throws IOException {
        if (value == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeSmallInt(value);
        }
    }

    @Override
    public void writeNullableString(@Nullable CharSequence value) throws IOException {
        if (value == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeString(value.toString());
        }
    }

    private class EncoderStream extends OutputStream {
        @Override
        public void write(byte[] buffer) throws IOException {
            writeBytes(buffer);
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            writeBytes(buffer, offset, length);
        }

        @Override
        public void write(int b) throws IOException {
            writeByte((byte) b);
        }
    }
}
