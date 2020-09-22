package seaweedfs.file;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

public class RandomeAccessFileTest {

    @Test
    public void testRandomWriteAndRead() throws IOException {

        File f = new File(MmapFileTest.dir, "mmap_file.txt");

        RandomAccessFile af = new RandomAccessFile(f, "rw");
        af.setLength(0);
        af.close();

        Random r = new Random();

        int maxLength = 5000;

        byte[] data = new byte[maxLength];
        byte[] readData = new byte[maxLength];

        for (int i = 4096; i < maxLength; i++) {

            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            long fileSize = raf.length();

            raf.readFully(readData, 0, (int)fileSize);

            for (int x=0;x<fileSize;x++){
                Assert.assertEquals(data[x], readData[x]);
            }

            int start = r.nextInt(i);
            int stop = r.nextInt(i);
            if (start > stop) {
                int t = stop;
                stop = start;
                start = t;
            }
            if (stop > fileSize) {
                fileSize = stop;
                raf.setLength(fileSize);
            }

            randomize(r, data, start, stop);
            raf.seek(start);
            raf.write(data, start, stop-start);

            raf.close();
        }

    }

    private static void randomize(Random r, byte[] bytes, int start, int stop) {
        for (int i = start; i < stop; i++) {
            int rnd = r.nextInt();
            bytes[i] = (byte) rnd;
        }
    }


}
