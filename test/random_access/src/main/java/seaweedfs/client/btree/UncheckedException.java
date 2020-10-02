/*
 * Copyright 2010 the original author or authors.
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
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;

/**
 * Wraps a checked exception. Carries no other context.
 */
public final class UncheckedException extends RuntimeException {
    private UncheckedException(Throwable cause) {
        super(cause);
    }

    private UncheckedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Note: always throws the failure in some form. The return value is to keep the compiler happy.
     */
    public static RuntimeException throwAsUncheckedException(Throwable t) {
        return throwAsUncheckedException(t, false);
    }

    /**
     * Note: always throws the failure in some form. The return value is to keep the compiler happy.
     */
    public static RuntimeException throwAsUncheckedException(Throwable t, boolean preserveMessage) {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        if (t instanceof IOException) {
            if (preserveMessage) {
                throw new UncheckedIOException(t.getMessage(), t);
            } else {
                throw new UncheckedIOException(t);
            }
        }
        if (preserveMessage) {
            throw new UncheckedException(t.getMessage(), t);
        } else {
            throw new UncheckedException(t);
        }
    }

    public static <T> T callUnchecked(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw throwAsUncheckedException(e);
        }
    }

    /**
     * Unwraps passed InvocationTargetException hence making the stack of exceptions cleaner without losing information.
     *
     * Note: always throws the failure in some form. The return value is to keep the compiler happy.
     *
     * @param e to be unwrapped
     * @return an instance of RuntimeException based on the target exception of the parameter.
     */
    public static RuntimeException unwrapAndRethrow(InvocationTargetException e) {
        return UncheckedException.throwAsUncheckedException(e.getTargetException());
    }
}
