/*
 * Copyright 2012 the original author or authors.
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

/**
 * Implementations must allow concurrent reading and writing, so that a thread can read and a thread can write at the same time.
 * Implementations do not need to support multiple read threads or multiple write threads.
 */
public interface StatefulSerializer<T> {
    /**
     * Should not perform any buffering
     */
    ObjectReader<T> newReader(Decoder decoder);

    /**
     * Should not perform any buffering
     */
    ObjectWriter<T> newWriter(Encoder encoder);
}
