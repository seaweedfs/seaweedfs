package seaweedfs.client;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SeaweedReadTest {

    @Test
    public void testNonOverlappingVisibleIntervals() {
        List<FilerProto.FileChunk> chunks = new ArrayList<>();
        chunks.add(FilerProto.FileChunk.newBuilder()
            .setFileId("aaa")
            .setOffset(0)
            .setSize(100)
            .setMtime(1000)
            .build());
        chunks.add(FilerProto.FileChunk.newBuilder()
            .setFileId("bbb")
            .setOffset(100)
            .setSize(133)
            .setMtime(2000)
            .build());

        List<SeaweedRead.VisibleInterval> visibleIntervals = SeaweedRead.nonOverlappingVisibleIntervals(chunks);
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

}
