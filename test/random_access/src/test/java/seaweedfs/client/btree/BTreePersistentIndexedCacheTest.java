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

import seaweedfs.client.btree.serialize.DefaultSerializer;
import seaweedfs.client.btree.serialize.Serializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class BTreePersistentIndexedCacheTest {
    private final Serializer<String> stringSerializer = new DefaultSerializer<String>();
    private final Serializer<Integer> integerSerializer = new DefaultSerializer<Integer>();
    private BTreePersistentIndexedCache<String, Integer> cache;
    private File cacheFile;

    @Before
    public void setup() {
        cacheFile = tmpDirFile("cache.bin");
    }

    public File tmpDirFile(String filename) {
        File f = new File("/Users/chris/tmp/mm/dev/btree_test");
        // File f = new File("/tmp/btree_test");
        f.mkdirs();
        return new File(f, filename);
    }

    private void createCache() {
        cache = new BTreePersistentIndexedCache<String, Integer>(cacheFile, stringSerializer, integerSerializer, (short) 4, 100);
    }

    private void verifyAndCloseCache() {
        cache.verify();
        cache.close();
    }

    @Test
    public void getReturnsNullWhenEntryDoesNotExist() {
        createCache();
        assertNull(cache.get("unknown"));
        verifyAndCloseCache();
    }

    @Test
    public void persistsAddedEntries() {
        createCache();
        checkAdds(1, 2, 3, 4, 5);
        verifyAndCloseCache();
    }

    @Test
    public void persistsAddedEntriesInReverseOrder() {
        createCache();
        checkAdds(5, 4, 3, 2, 1);
        verifyAndCloseCache();
    }

    @Test
    public void persistsAddedEntriesOverMultipleIndexBlocks() {
        createCache();
        checkAdds(3, 2, 11, 5, 7, 1, 10, 8, 9, 4, 6, 0);
        verifyAndCloseCache();
    }

    @Test
    public void persistsUpdates() {
        createCache();
        checkUpdates(3, 2, 11, 5, 7, 1, 10, 8, 9, 4, 6, 0);
        verifyAndCloseCache();
    }

    @Test
    public void handlesUpdatesWhenBlockSizeDecreases() {
        BTreePersistentIndexedCache<String, List<Integer>> cache =
                new BTreePersistentIndexedCache<String, List<Integer>>(
                        tmpDirFile("listcache.bin"), stringSerializer,
                        new DefaultSerializer<List<Integer>>(), (short) 4, 100);

        List<Integer> values = Arrays.asList(3, 2, 11, 5, 7, 1, 10, 8, 9, 4, 6, 0);
        Map<Integer, List<Integer>> updated = new LinkedHashMap<Integer, List<Integer>>();

        for (int i = 10; i > 0; i--) {
            for (Integer value : values) {
                String key = String.format("key_%d", value);
                List<Integer> newValue = new ArrayList<Integer>(i);
                for (int j = 0; j < i * 2; j++) {
                    newValue.add(j);
                }
                cache.put(key, newValue);
                updated.put(value, newValue);
            }

            checkListEntries(cache, updated);
        }

        cache.reset();

        checkListEntries(cache, updated);

        cache.verify();
        cache.close();
    }

    private void checkListEntries(BTreePersistentIndexedCache<String, List<Integer>> cache, Map<Integer, List<Integer>> updated) {
        for (Map.Entry<Integer, List<Integer>> entry : updated.entrySet()) {
            String key = String.format("key_%d", entry.getKey());
            assertThat(cache.get(key), equalTo(entry.getValue()));
        }
    }

    @Test
    public void handlesUpdatesWhenBlockSizeIncreases() {
        BTreePersistentIndexedCache<String, List<Integer>> cache =
                new BTreePersistentIndexedCache<String, List<Integer>>(
                        tmpDirFile("listcache.bin"), stringSerializer,
                        new DefaultSerializer<List<Integer>>(), (short) 4, 100);

        List<Integer> values = Arrays.asList(3, 2, 11, 5, 7, 1, 10, 8, 9, 4, 6, 0);
        Map<Integer, List<Integer>> updated = new LinkedHashMap<Integer, List<Integer>>();

        for (int i = 1; i < 10; i++) {
            for (Integer value : values) {
                String key = String.format("key_%d", value);
                List<Integer> newValue = new ArrayList<Integer>(i);
                for (int j = 0; j < i * 2; j++) {
                    newValue.add(j);
                }
                cache.put(key, newValue);
                updated.put(value, newValue);
            }

            checkListEntries(cache, updated);
        }

        cache.reset();

        checkListEntries(cache, updated);

        cache.verify();
        cache.close();
    }

    @Test
    public void persistsAddedEntriesAfterReopen() {
        createCache();

        checkAdds(1, 2, 3, 4);

        cache.reset();

        checkAdds(5, 6, 7, 8);
        verifyAndCloseCache();
    }

    @Test
    public void persistsReplacedEntries() {
        createCache();

        cache.put("key_1", 1);
        cache.put("key_2", 2);
        cache.put("key_3", 3);
        cache.put("key_4", 4);
        cache.put("key_5", 5);

        cache.put("key_1", 1);
        cache.put("key_4", 12);

        assertThat(cache.get("key_1"), equalTo(1));
        assertThat(cache.get("key_2"), equalTo(2));
        assertThat(cache.get("key_3"), equalTo(3));
        assertThat(cache.get("key_4"), equalTo(12));
        assertThat(cache.get("key_5"), equalTo(5));

        cache.reset();

        assertThat(cache.get("key_1"), equalTo(1));
        assertThat(cache.get("key_2"), equalTo(2));
        assertThat(cache.get("key_3"), equalTo(3));
        assertThat(cache.get("key_4"), equalTo(12));
        assertThat(cache.get("key_5"), equalTo(5));

        verifyAndCloseCache();
    }

    @Test
    public void reusesEmptySpaceWhenPuttingEntries() {
        BTreePersistentIndexedCache<String, String> cache = new BTreePersistentIndexedCache<String, String>(cacheFile, stringSerializer, stringSerializer, (short) 4, 100);

        long beforeLen = cacheFile.length();
        if (beforeLen>0){
            System.out.println(String.format("cache %s: %s", "key_new", cache.get("key_new")));
        }

        cache.put("key_1", "abcd");
        cache.put("key_2", "abcd");
        cache.put("key_3", "abcd");
        cache.put("key_4", "abcd");
        cache.put("key_5", "abcd");

        long len = cacheFile.length();
        assertTrue(len > 0L);

        System.out.println(String.format("cache file size %d => %d", beforeLen, len));

        cache.put("key_1", "1234");
        assertThat(cacheFile.length(), equalTo(len));

        cache.remove("key_1");
        cache.put("key_new", "a1b2");
        assertThat(cacheFile.length(), equalTo(len));

        cache.put("key_new", "longer value assertThat(cacheFile.length(), equalTo(len))");
        System.out.println(String.format("cache file size %d beforeLen %d", cacheFile.length(), len));
        // assertTrue(cacheFile.length() > len);
        len = cacheFile.length();

        cache.put("key_1", "1234");
        assertThat(cacheFile.length(), equalTo(len));

        cache.close();
    }

    @Test
    public void canHandleLargeNumberOfEntries() {
        createCache();
        int count = 2000;
        List<Integer> values = new ArrayList<Integer>();
        for (int i = 0; i < count; i++) {
            values.add(i);
        }

        checkAddsAndRemoves(null, values);

        long len = cacheFile.length();

        checkAddsAndRemoves(Collections.reverseOrder(), values);

        // need to make this better
        assertTrue(cacheFile.length() < (long)(1.4 * len));

        checkAdds(values);

        // need to make this better
        assertTrue(cacheFile.length() < (long) (1.4 * 1.4 * len));

        cache.close();
    }

    @Test
    public void persistsRemovalOfEntries() {
        createCache();
        checkAddsAndRemoves(1, 2, 3, 4, 5);
        verifyAndCloseCache();
    }

    @Test
    public void persistsRemovalOfEntriesInReverse() {
        createCache();
        checkAddsAndRemoves(Collections.<Integer>reverseOrder(), 1, 2, 3, 4, 5);
        verifyAndCloseCache();
    }

    @Test
    public void persistsRemovalOfEntriesOverMultipleIndexBlocks() {
        createCache();
        checkAddsAndRemoves(4, 12, 9, 1, 3, 10, 11, 7, 8, 2, 5, 6);
        verifyAndCloseCache();
    }

    @Test
    public void removalRedistributesRemainingEntriesWithLeftSibling() {
        createCache();
        // Ends up with: 1 2 3 -> 4 <- 5 6
        checkAdds(1, 2, 5, 6, 4, 3);
        cache.verify();
        cache.remove("key_5");
        verifyAndCloseCache();
    }

    @Test
    public void removalMergesRemainingEntriesIntoLeftSibling() {
        createCache();
        // Ends up with: 1 2 -> 3 <- 4 5
        checkAdds(1, 2, 4, 5, 3);
        cache.verify();
        cache.remove("key_4");
        verifyAndCloseCache();
    }

    @Test
    public void removalRedistributesRemainingEntriesWithRightSibling() {
        createCache();
        // Ends up with: 1 2 -> 3 <- 4 5 6
        checkAdds(1, 2, 4, 5, 3, 6);
        cache.verify();
        cache.remove("key_2");
        verifyAndCloseCache();
    }

    @Test
    public void removalMergesRemainingEntriesIntoRightSibling() {
        createCache();
        // Ends up with: 1 2 -> 3 <- 4 5
        checkAdds(1, 2, 4, 5, 3);
        cache.verify();
        cache.remove("key_2");
        verifyAndCloseCache();
    }

    @Test
    public void handlesOpeningATruncatedCacheFile() throws IOException {
        BTreePersistentIndexedCache<String, Integer> cache = new BTreePersistentIndexedCache<String, Integer>(cacheFile, stringSerializer, integerSerializer);

        assertNull(cache.get("key_1"));
        cache.put("key_1", 99);

        RandomAccessFile file = new RandomAccessFile(cacheFile, "rw");
        file.setLength(file.length() - 10);
        file.close();

        cache.reset();

        assertNull(cache.get("key_1"));
        cache.verify();

        cache.close();
    }

    @Test
    public void canUseFileAsKey() {
        BTreePersistentIndexedCache<File, Integer> cache = new BTreePersistentIndexedCache<File, Integer>(cacheFile, new DefaultSerializer<File>(), integerSerializer);

        cache.put(new File("file"), 1);
        cache.put(new File("dir/file"), 2);
        cache.put(new File("File"), 3);

        assertThat(cache.get(new File("file")), equalTo(1));
        assertThat(cache.get(new File("dir/file")), equalTo(2));
        assertThat(cache.get(new File("File")), equalTo(3));

        cache.close();
    }

    @Test
    public void handlesKeysWithSameHashCode() {
        createCache();

        String key1 = new String(new byte[]{2, 31});
        String key2 = new String(new byte[]{1, 62});
        cache.put(key1, 1);
        cache.put(key2, 2);

        assertThat(cache.get(key1), equalTo(1));
        assertThat(cache.get(key2), equalTo(2));

        cache.close();
    }

    private void checkAdds(Integer... values) {
        checkAdds(Arrays.asList(values));
    }

    private Map<String, Integer> checkAdds(Iterable<Integer> values) {
        Map<String, Integer> added = new LinkedHashMap<String, Integer>();

        for (Integer value : values) {
            String key = String.format("key_%d", value);
            cache.put(key, value);
            added.put(String.format("key_%d", value), value);
        }

        for (Map.Entry<String, Integer> entry : added.entrySet()) {
            assertThat(cache.get(entry.getKey()), equalTo(entry.getValue()));
        }

        cache.reset();

        for (Map.Entry<String, Integer> entry : added.entrySet()) {
            assertThat(cache.get(entry.getKey()), equalTo(entry.getValue()));
        }

        return added;
    }

    private void checkUpdates(Integer... values) {
        checkUpdates(Arrays.asList(values));
    }

    private Map<Integer, Integer> checkUpdates(Iterable<Integer> values) {
        Map<Integer, Integer> updated = new LinkedHashMap<Integer, Integer>();

        for (int i = 0; i < 10; i++) {
            for (Integer value : values) {
                String key = String.format("key_%d", value);
                int newValue = value + (i * 100);
                cache.put(key, newValue);
                updated.put(value, newValue);
            }

            for (Map.Entry<Integer, Integer> entry : updated.entrySet()) {
                String key = String.format("key_%d", entry.getKey());
                assertThat(cache.get(key), equalTo(entry.getValue()));
            }
        }

        cache.reset();

        for (Map.Entry<Integer, Integer> entry : updated.entrySet()) {
            String key = String.format("key_%d", entry.getKey());
            assertThat(cache.get(key), equalTo(entry.getValue()));
        }

        return updated;
    }

    private void checkAddsAndRemoves(Integer... values) {
        checkAddsAndRemoves(null, values);
    }

    private void checkAddsAndRemoves(Comparator<Integer> comparator, Integer... values) {
        checkAddsAndRemoves(comparator, Arrays.asList(values));
    }

    private void checkAddsAndRemoves(Comparator<Integer> comparator, Collection<Integer> values) {
        checkAdds(values);

        List<Integer> deleteValues = new ArrayList<Integer>(values);
        Collections.sort(deleteValues, comparator);
        for (Integer value : deleteValues) {
            String key = String.format("key_%d", value);
            assertThat(cache.get(key), notNullValue());
            cache.remove(key);
            assertThat(cache.get(key), nullValue());
        }

        cache.reset();
        cache.verify();

        for (Integer value : deleteValues) {
            String key = String.format("key_%d", value);
            assertThat(cache.get(key), nullValue());
        }
    }

}
