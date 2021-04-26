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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Provides a way to decode structured data from a backing byte stream. Implementations may buffer incoming bytes read
 * from the backing stream prior to decoding.
 */
public interface Decoder {
    /**
     * Returns an InputStream which can be used to read raw bytes.
     */
    InputStream getInputStream();

    /**
     * Reads a signed 64 bit long value. Can read any value that was written using {@link Encoder#writeLong(long)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the long value can be fully read.
     */
    long readLong() throws EOFException, IOException;

    /**
     * Reads a signed 64 bit int value. Can read any value that was written using {@link Encoder#writeSmallLong(long)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the int value can be fully read.
     */
    long readSmallLong() throws EOFException, IOException;

    /**
     * Reads a signed 32 bit int value. Can read any value that was written using {@link Encoder#writeInt(int)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the int value can be fully read.
     */
    int readInt() throws EOFException, IOException;

    /**
     * Reads a signed 32 bit int value. Can read any value that was written using {@link Encoder#writeSmallInt(int)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the int value can be fully read.
     */
    int readSmallInt() throws EOFException, IOException;

    /**
     * Reads a nullable signed 32 bit int value.
     *
     * @see #readSmallInt()
     */
    @Nullable
    Integer readNullableSmallInt() throws EOFException, IOException;

    /**
     * Reads a boolean value. Can read any value that was written using {@link Encoder#writeBoolean(boolean)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the boolean value can be fully read.
     */
    boolean readBoolean() throws EOFException, IOException;

    /**
     * Reads a non-null string value. Can read any value that was written using {@link Encoder#writeString(CharSequence)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the string can be fully read.
     */
    String readString() throws EOFException, IOException;

    /**
     * Reads a nullable string value. Can reads any value that was written using {@link Encoder#writeNullableString(CharSequence)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the string can be fully read.
     */
    @Nullable
    String readNullableString() throws EOFException, IOException;

    /**
     * Reads a byte value. Can read any byte value that was written using one of the raw byte methods on {@link Encoder}, such as {@link Encoder#writeByte(byte)} or {@link Encoder#getOutputStream()}
     *
     * @throws EOFException when the end of the byte stream is reached.
     */
    byte readByte() throws EOFException, IOException;

    /**
     * Reads bytes into the given buffer, filling the buffer. Can read any byte values that were written using one of the raw byte methods on {@link Encoder}, such as {@link
     * Encoder#writeBytes(byte[])} or {@link Encoder#getOutputStream()}
     *
     * @throws EOFException when the end of the byte stream is reached before the buffer is full.
     */
    void readBytes(byte[] buffer) throws EOFException, IOException;

    /**
     * Reads the specified number of bytes into the given buffer. Can read any byte values that were written using one of the raw byte methods on {@link Encoder}, such as {@link
     * Encoder#writeBytes(byte[])} or {@link Encoder#getOutputStream()}
     *
     * @throws EOFException when the end of the byte stream is reached before the specified number of bytes were read.
     */
    void readBytes(byte[] buffer, int offset, int count) throws EOFException, IOException;

    /**
     * Reads a byte array. Can read any byte array written using {@link Encoder#writeBinary(byte[])} or {@link Encoder#writeBinary(byte[], int, int)}.
     *
     * @throws EOFException when the end of the byte stream is reached before the byte array was fully read.
     */
    byte[] readBinary() throws EOFException, IOException;

    /**
     * Skips the given number of bytes. Can skip over any byte values that were written using one of the raw byte methods on {@link Encoder}.
     */
    void skipBytes(long count) throws EOFException, IOException;

    /**
     * Reads a byte stream written using {@link Encoder#encodeChunked(Encoder.EncodeAction)}.
     */
    <T> T decodeChunked(DecodeAction<Decoder, T> decodeAction) throws EOFException, Exception;

    /**
     * Skips over a byte stream written using {@link Encoder#encodeChunked(Encoder.EncodeAction)}, discarding its content.
     */
    void skipChunked() throws EOFException, IOException;

    interface DecodeAction<IN, OUT> {
        OUT read(IN source) throws Exception;
    }
}
