package seaweedfs.client;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SeaweedReadTest {

    @Test
    public void testNonOverlappingVisibleIntervals() throws IOException {
        List<FilerProto.FileChunk> chunks = new ArrayList<>();
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("aaa")
                .setOffset(0)
                .setSize(100)
                .setModifiedTsNs(1000)
                .build());
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("bbb")
                .setOffset(100)
                .setSize(133)
                .setModifiedTsNs(2000)
                .build());

        List<SeaweedRead.VisibleInterval> visibleIntervals = SeaweedRead.nonOverlappingVisibleIntervals(null, chunks);
        for (SeaweedRead.VisibleInterval visibleInterval : visibleIntervals) {
            System.out.println("visible:" + visibleInterval);
        }

        Assert.assertEquals(visibleIntervals.size(), 2);

        SeaweedRead.VisibleInterval visibleInterval = visibleIntervals.get(0);
        Assert.assertEquals(visibleInterval.start, 0);
        Assert.assertEquals(visibleInterval.stop, 100);
        Assert.assertEquals(visibleInterval.modifiedTime, 1000);
        Assert.assertEquals(visibleInterval.fileId, "aaa");

        visibleInterval = visibleIntervals.get(1);
        Assert.assertEquals(visibleInterval.start, 100);
        Assert.assertEquals(visibleInterval.stop, 233);
        Assert.assertEquals(visibleInterval.modifiedTime, 2000);
        Assert.assertEquals(visibleInterval.fileId, "bbb");

        List<SeaweedRead.ChunkView> chunkViews = SeaweedRead.viewFromVisibles(visibleIntervals, 0, 233);

        SeaweedRead.ChunkView chunkView = chunkViews.get(0);
        Assert.assertEquals(chunkView.offset, 0);
        Assert.assertEquals(chunkView.size, 100);
        Assert.assertEquals(chunkView.logicOffset, 0);
        Assert.assertEquals(chunkView.fileId, "aaa");

        chunkView = chunkViews.get(1);
        Assert.assertEquals(chunkView.offset, 0);
        Assert.assertEquals(chunkView.size, 133);
        Assert.assertEquals(chunkView.logicOffset, 100);
        Assert.assertEquals(chunkView.fileId, "bbb");


    }


    @Test
    public void testReadResolvedChunks() throws IOException {
        List<FilerProto.FileChunk> chunks = new ArrayList<>();
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("a")
                .setOffset(0)
                .setSize(100)
                .setModifiedTsNs(1)
                .build());
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("b")
                .setOffset(50)
                .setSize(100)
                .setModifiedTsNs(2)
                .build());
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("c")
                .setOffset(200)
                .setSize(50)
                .setModifiedTsNs(3)
                .build());
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("d")
                .setOffset(250)
                .setSize(50)
                .setModifiedTsNs(4)
                .build());
        chunks.add(FilerProto.FileChunk.newBuilder()
                .setFileId("e")
                .setOffset(175)
                .setSize(100)
                .setModifiedTsNs(5)
                .build());

        List<SeaweedRead.VisibleInterval> visibleIntervals = ReadChunks.readResolvedChunks(chunks);
        for (SeaweedRead.VisibleInterval visibleInterval : visibleIntervals) {
            System.out.println("visible:" + visibleInterval);
        }

        Assert.assertEquals(4, visibleIntervals.size());

        SeaweedRead.VisibleInterval visibleInterval = visibleIntervals.get(0);
        Assert.assertEquals(visibleInterval.start, 0);
        Assert.assertEquals(visibleInterval.stop, 50);
        Assert.assertEquals(visibleInterval.modifiedTime, 1);
        Assert.assertEquals(visibleInterval.fileId, "a");

        visibleInterval = visibleIntervals.get(1);
        Assert.assertEquals(visibleInterval.start, 50);
        Assert.assertEquals(visibleInterval.stop, 150);
        Assert.assertEquals(visibleInterval.modifiedTime, 2);
        Assert.assertEquals(visibleInterval.fileId, "b");

        visibleInterval = visibleIntervals.get(2);
        Assert.assertEquals(visibleInterval.start, 175);
        Assert.assertEquals(visibleInterval.stop, 275);
        Assert.assertEquals(visibleInterval.modifiedTime, 5);
        Assert.assertEquals(visibleInterval.fileId, "e");

        visibleInterval = visibleIntervals.get(3);
        Assert.assertEquals(visibleInterval.start, 275);
        Assert.assertEquals(visibleInterval.stop, 300);
        Assert.assertEquals(visibleInterval.modifiedTime, 4);
        Assert.assertEquals(visibleInterval.fileId, "d");

    }


    @Test
    public void testRandomizedReadResolvedChunks() throws IOException {
        Random random = new Random();
        int limit = 1024*1024;
        long[] array = new long[limit];
        List<FilerProto.FileChunk> chunks = new ArrayList<>();
        for (long ts=0;ts<1024;ts++){
            int x = random.nextInt(limit);
            int y = random.nextInt(limit);
            int size = Math.min(Math.abs(x-y), 1024);
            chunks.add(randomWrite(array, Math.min(x,y), size, ts));
        }

        List<SeaweedRead.VisibleInterval> visibleIntervals = ReadChunks.readResolvedChunks(chunks);
        for (SeaweedRead.VisibleInterval visibleInterval : visibleIntervals) {
            System.out.println("visible:" + visibleInterval);
            for (int i = (int) visibleInterval.start; i<visibleInterval.stop; i++) {
                Assert.assertEquals(array[i], visibleInterval.modifiedTime);
            }
        }

    }
    private FilerProto.FileChunk randomWrite(long[] array, int start, int size, long ts) {
        for (int i=start;i<start+size;i++) {
            array[i] = ts;
        }
        return FilerProto.FileChunk.newBuilder()
                .setFileId("")
                .setOffset(start)
                .setSize(size)
                .setModifiedTsNs(ts)
                .build();
    }
}
