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

import javax.annotation.Nullable;

public abstract class Cast {

    /**
     * Casts the given object to the given type, providing a better error message than the default.
     *
     * The standard {@link Class#cast(Object)} method produces unsatisfactory error messages on some platforms
     * when it fails. All this method does is provide a better, consistent, error message.
     *
     * This should be used whenever there is a chance the cast could fail. If in doubt, use this.
     *
     * @param outputType The type to cast the input to
     * @param object The object to be cast (must not be {@code null})
     * @param <O> The type to be cast to
     * @param <I> The type of the object to be vast
     * @return The input object, cast to the output type
     */
    public static <O, I> O cast(Class<O> outputType, I object) {
        try {
            return outputType.cast(object);
        } catch (ClassCastException e) {
            throw new ClassCastException(String.format(
                    "Failed to cast object %s of type %s to target type %s", object, object.getClass().getName(), outputType.getName()
            ));
        }
    }

    /**
     * Casts the given object to the given type, providing a better error message than the default.
     *
     * The standard {@link Class#cast(Object)} method produces unsatisfactory error messages on some platforms
     * when it fails. All this method does is provide a better, consistent, error message.
     *
     * This should be used whenever there is a chance the cast could fail. If in doubt, use this.
     *
     * @param outputType The type to cast the input to
     * @param object The object to be cast
     * @param <O> The type to be cast to
     * @param <I> The type of the object to be vast
     * @return The input object, cast to the output type
     */
    @Nullable
    public static <O, I> O castNullable(Class<O> outputType, @Nullable I object) {
        if (object == null) {
            return null;
        }
        return cast(outputType, object);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T uncheckedCast(@Nullable Object object) {
        return (T) object;
    }

    @SuppressWarnings("unchecked")
    public static <T> T uncheckedNonnullCast(Object object) {
        return (T) object;
    }
}
