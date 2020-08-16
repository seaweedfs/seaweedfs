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

package seaweedfs.client.btree.serialize.kryo;

import seaweedfs.client.btree.serialize.*;

public class TypeSafeSerializer<T> implements StatefulSerializer<Object> {
    private final Class<T> type;
    private final StatefulSerializer<T> serializer;

    public TypeSafeSerializer(Class<T> type, StatefulSerializer<T> serializer) {
        this.type = type;
        this.serializer = serializer;
    }

    @Override
    public ObjectReader<Object> newReader(Decoder decoder) {
        final ObjectReader<T> reader = serializer.newReader(decoder);
        return new ObjectReader<Object>() {
            @Override
            public Object read() throws Exception {
                return reader.read();
            }
        };
    }

    @Override
    public ObjectWriter<Object> newWriter(Encoder encoder) {
        final ObjectWriter<T> writer = serializer.newWriter(encoder);
        return new ObjectWriter<Object>() {
            @Override
            public void write(Object value) throws Exception {
                writer.write(type.cast(value));
            }
        };
    }
}
