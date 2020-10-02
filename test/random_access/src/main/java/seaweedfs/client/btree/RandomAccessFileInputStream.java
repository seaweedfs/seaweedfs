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
package seaweedfs.client.btree;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * Reads from a {@link RandomAccessFile}. Each operation reads from and advances the current position of the file.
 *
 * <p>Closing this stream does not close the underlying file.
 */
public class RandomAccessFileInputStream extends InputStream {
    private final RandomAccessFile file;

    public RandomAccessFileInputStream(RandomAccessFile file) {
        this.file = file;
    }

    @Override
    public long skip(long n) throws IOException {
        file.seek(file.getFilePointer() + n);
        return n;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return file.read(bytes);
    }

    @Override
    public int read() throws IOException {
        return file.read();
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        return file.read(bytes, offset, length);
    }
}
