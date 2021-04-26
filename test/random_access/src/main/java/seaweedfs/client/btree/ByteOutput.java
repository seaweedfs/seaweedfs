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

import com.google.common.io.CountingOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * Allows a stream of bytes to be written to a particular location of some backing byte stream.
 */
class ByteOutput {
    private final RandomAccessFile file;
    private final ResettableBufferedOutputStream bufferedOutputStream;
    private CountingOutputStream countingOutputStream;

    public ByteOutput(RandomAccessFile file) {
        this.file = file;
        bufferedOutputStream = new ResettableBufferedOutputStream(new RandomAccessFileOutputStream(file));
    }

    /**
     * Starts writing to the given offset. Can be beyond the current length of the file.
     */
    public DataOutputStream start(long offset) throws IOException {
        file.seek(offset);
        bufferedOutputStream.clear();
        countingOutputStream = new CountingOutputStream(bufferedOutputStream);
        return new DataOutputStream(countingOutputStream);
    }

    /**
     * Returns the number of byte written since {@link #start(long)} was called.
     */
    public long getBytesWritten() {
        return countingOutputStream.getCount();
    }

    /**
     * Finishes writing, flushing and resetting any buffered state
     */
    public void done() throws IOException {
        countingOutputStream.flush();
        countingOutputStream = null;
    }

    private static class ResettableBufferedOutputStream extends BufferedOutputStream {
        ResettableBufferedOutputStream(OutputStream output) {
            super(output);
        }

        void clear() {
            count = 0;
        }
    }
}
