package seaweed.hdfs;

import org.junit.Test;
import seaweedfs.client.FilerProto;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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

        assertEquals(visibleIntervals.size(), 2);

        SeaweedRead.VisibleInterval visibleInterval = visibleIntervals.get(0);
        assertEquals(visibleInterval.start, 0);
        assertEquals(visibleInterval.stop, 100);
        assertEquals(visibleInterval.modifiedTime, 1000);
        assertEquals(visibleInterval.fileId, "aaa");

        visibleInterval = visibleIntervals.get(1);
        assertEquals(visibleInterval.start, 100);
        assertEquals(visibleInterval.stop, 233);
        assertEquals(visibleInterval.modifiedTime, 2000);
        assertEquals(visibleInterval.fileId, "bbb");

        List<SeaweedRead.ChunkView> chunkViews = SeaweedRead.viewFromVisibles(visibleIntervals, 0, 233);

        SeaweedRead.ChunkView chunkView = chunkViews.get(0);
        assertEquals(chunkView.offset, 0);
        assertEquals(chunkView.size, 100);
        assertEquals(chunkView.logicOffset, 0);
        assertEquals(chunkView.fileId, "aaa");

        chunkView = chunkViews.get(1);
        assertEquals(chunkView.offset, 0);
        assertEquals(chunkView.size, 133);
        assertEquals(chunkView.logicOffset, 100);
        assertEquals(chunkView.fileId, "bbb");


    }

}
