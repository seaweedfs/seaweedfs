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

public abstract class Block {
    static final int LONG_SIZE = 8;
    static final int INT_SIZE = 4;
    static final int SHORT_SIZE = 2;

    private BlockPayload payload;

    protected Block(BlockPayload payload) {
        this.payload = payload;
        payload.setBlock(this);
    }

    public BlockPayload getPayload() {
        return payload;
    }

    protected void detach() {
        payload.setBlock(null);
        payload = null;
    }

    public abstract BlockPointer getPos();

    public abstract int getSize();

    public abstract RuntimeException blockCorruptedException();

    @Override
    public String toString() {
        return payload.getClass().getSimpleName() + " " + getPos();
    }

    public BlockPointer getNextPos() {
        return BlockPointer.pos(getPos().getPos() + getSize());
    }

    public abstract boolean hasPos();

    public abstract void setPos(BlockPointer pos);

    public abstract void setSize(int size);
}
