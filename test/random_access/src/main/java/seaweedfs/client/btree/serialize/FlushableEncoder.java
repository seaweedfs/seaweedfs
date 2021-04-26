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

import java.io.Flushable;
import java.io.IOException;

/**
 * Represents an {@link Encoder} that buffers encoded data prior to writing to the backing stream.
 */
public interface FlushableEncoder extends Encoder, Flushable {
    /**
     * Ensures that all buffered data has been written to the backing stream. Does not flush the backing stream.
     */
    @Override
    void flush() throws IOException;
}
