/*
 * Copyright 2014 the original author or authors.
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

import com.google.common.io.CountingInputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * Allows a stream of bytes to be read from a particular location of some backing byte stream.
 */
class ByteInput {
    private final RandomAccessFile file;
    private final ResettableBufferedInputStream bufferedInputStream;
    private CountingInputStream countingInputStream;

    public ByteInput(RandomAccessFile file) {
        this.file = file;
        bufferedInputStream = new ResettableBufferedInputStream(new RandomAccessFileInputStream(file));
    }

    /**
     * Starts reading from the given offset.
     */
    public DataInputStream start(long offset) throws IOException {
        file.seek(offset);
        bufferedInputStream.clear();
        countingInputStream = new CountingInputStream(bufferedInputStream);
        return new DataInputStream(countingInputStream);
    }

    /**
     * Returns the number of bytes read since {@link #start(long)} was called.
     */
    public long getBytesRead() {
        return countingInputStream.getCount();
    }

    /**
     * Finishes reading, resetting any buffered state.
     */
    public void done() {
        countingInputStream = null;
    }

    private static class ResettableBufferedInputStream extends BufferedInputStream {
        ResettableBufferedInputStream(InputStream input) {
            super(input);
        }

        void clear() {
            count = 0;
            pos = 0;
        }
    }
}
