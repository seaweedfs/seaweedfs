package seaweedfs.hdfs;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import seaweed.hdfs.SeaweedRead;
import seaweedfs.client.FilerProto;

import java.io.IOException;
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

    // test gzipped content with range requests. Conclusion: not doing this.
    public void testGzippedRangeRequest() throws IOException {

        SeaweedRead.ChunkView chunkView = new SeaweedRead.ChunkView("2,621a042be6e39d", 0, 28, 0);
        CloseableHttpClient client = HttpClientBuilder.create().build();
        String targetUrl = String.format("http://%s/%s", "localhost:8080", chunkView.fileId);
        HttpGet request = new HttpGet(targetUrl);
        // request.removeHeaders(HttpHeaders.ACCEPT_ENCODING);
        request.setHeader(HttpHeaders.ACCEPT_ENCODING, "");
        request.setHeader(HttpHeaders.RANGE, String.format("bytes=%d-%d", chunkView.offset, chunkView.offset + chunkView.size));
        System.out.println("request:");
        for (Header header : request.getAllHeaders()) {
            System.out.println(header.getName() + ": " + header.getValue());
        }

        int len = 29;
        byte[] buffer = new byte[len];
        CloseableHttpResponse response = null;
        try {
            response = client.execute(request);
            HttpEntity entity = response.getEntity();
            System.out.println("content length:" + entity.getContentLength());
            System.out.println("is streaming:" + entity.isStreaming());
            System.out.println(EntityUtils.toString(entity));
        } finally {

            if (response != null) {
                response.close();
            }
        }

    }
}
