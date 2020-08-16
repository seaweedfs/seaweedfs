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
package seaweedfs.client.btree;

import com.google.common.primitives.Longs;

public class BlockPointer implements Comparable<BlockPointer> {

    private static final BlockPointer NULL = new BlockPointer(-1);

    public static BlockPointer start() {
        return NULL;
    }

    public static BlockPointer pos(long pos) {
        if (pos < -1) {
            throw new CorruptedCacheException("block pointer must be >= -1, but was" + pos);
        }
        if (pos == -1) {
            return NULL;
        }
        return new BlockPointer(pos);
    }

    private final long pos;

    private BlockPointer(long pos) {
        this.pos = pos;
    }

    public boolean isNull() {
        return pos < 0;
    }

    public long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return String.valueOf(pos);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BlockPointer other = (BlockPointer) obj;
        return pos == other.pos;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(pos);
    }

    @Override
    public int compareTo(BlockPointer o) {
        return Longs.compare(pos, o.pos);
    }
}
