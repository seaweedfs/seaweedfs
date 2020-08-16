/*
 * Copyright 2016 the original author or authors.
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

import com.google.common.base.Objects;

/**
 * This abstract class provide a sensible default implementation for {@code Serializer} equality. This equality
 * implementation is required to enable cache instance reuse within the same Gradle runtime. Serializers are used
 * as cache parameter which need to be compared to determine compatible cache.
 */
public abstract class AbstractSerializer<T> implements Serializer<T> {
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        return Objects.equal(obj.getClass(), getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getClass());
    }
}
