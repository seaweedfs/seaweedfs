/*
 * Copyright 2014 the original author or authors.
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

import seaweedfs.client.btree.serialize.Serializer;
import seaweedfs.client.btree.serialize.kryo.KryoBackedEncoder;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class KeyHasher<K> {
    private final Serializer<K> serializer;
    private final MessageDigestStream digestStream = new MessageDigestStream();
    private final KryoBackedEncoder encoder = new KryoBackedEncoder(digestStream);

    public KeyHasher(Serializer<K> serializer) {
        this.serializer = serializer;
    }

    long getHashCode(K key) throws Exception {
        serializer.write(encoder, key);
        encoder.flush();
        return digestStream.getChecksum();
    }

    private static class MessageDigestStream extends OutputStream {
        MessageDigest messageDigest;

        private MessageDigestStream() {
            try {
                messageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw UncheckedException.throwAsUncheckedException(e);
            }
        }

        @Override
        public void write(int b) throws IOException {
            messageDigest.update((byte) b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            messageDigest.update(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            messageDigest.update(b, off, len);
        }

        long getChecksum() {
            byte[] digest = messageDigest.digest();
            assert digest.length == 16;
            return new BigInteger(digest).longValue();
        }
    }
}
