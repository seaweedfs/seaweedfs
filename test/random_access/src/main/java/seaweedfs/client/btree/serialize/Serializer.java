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
package seaweedfs.client.btree.serialize;

import java.io.EOFException;

public interface Serializer<T> {
    /**
     * Reads the next object from the given stream. The implementation must not perform any buffering, so that it reads only those bytes from the input stream that are
     * required to deserialize the next object.
     *
     * @throws EOFException When the next object cannot be fully read due to reaching the end of stream.
     */
    T read(Decoder decoder) throws EOFException, Exception;

    /**
     * Writes the given object to the given stream. The implementation must not perform any buffering.
     */
    void write(Encoder encoder, T value) throws Exception;
}
