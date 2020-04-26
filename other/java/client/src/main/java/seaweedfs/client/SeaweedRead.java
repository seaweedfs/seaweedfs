package seaweedfs.client;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class SeaweedRead {

    // private static final Logger LOG = LoggerFactory.getLogger(SeaweedRead.class);

    static ChunkCache chunkCache = new ChunkCache(1000);

    // returns bytesRead
    public static long read(FilerGrpcClient filerGrpcClient, List<VisibleInterval> visibleIntervals,
                            final long position, final byte[] buffer, final int bufferOffset,
                            final int bufferLength) throws IOException {

        List<ChunkView> chunkViews = viewFromVisibles(visibleIntervals, position, bufferLength);

        FilerProto.LookupVolumeRequest.Builder lookupRequest = FilerProto.LookupVolumeRequest.newBuilder();
        for (ChunkView chunkView : chunkViews) {
            String vid = parseVolumeId(chunkView.fileId);
            lookupRequest.addVolumeIds(vid);
        }

        FilerProto.LookupVolumeResponse lookupResponse = filerGrpcClient
                .getBlockingStub().lookupVolume(lookupRequest.build());

        Map<String, FilerProto.Locations> vid2Locations = lookupResponse.getLocationsMapMap();

        //TODO parallel this
        long readCount = 0;
        int startOffset = bufferOffset;
        for (ChunkView chunkView : chunkViews) {
            FilerProto.Locations locations = vid2Locations.get(parseVolumeId(chunkView.fileId));
            if (locations.getLocationsCount() == 0) {
                // log here!
                return 0;
            }

            int len = readChunkView(position, buffer, startOffset, chunkView, locations);

            readCount += len;
            startOffset += len;

        }

        return readCount;
    }

    private static int readChunkView(long position, byte[] buffer, int startOffset, ChunkView chunkView, FilerProto.Locations locations) throws IOException {

        byte[] chunkData = chunkCache.getChunk(chunkView.fileId);

        if (chunkData == null) {
            chunkData = doFetchFullChunkData(chunkView, locations);
        }

        int len = (int) (chunkView.logicOffset - position + chunkView.size);
        System.arraycopy(chunkData, (int) chunkView.offset, buffer, startOffset, len);

        chunkCache.setChunk(chunkView.fileId, chunkData);

        return len;
    }

    private static byte[] doFetchFullChunkData(ChunkView chunkView, FilerProto.Locations locations) throws IOException {

        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(
                String.format("http://%s/%s", locations.getLocations(0).getUrl(), chunkView.fileId));

        request.setHeader(HttpHeaders.ACCEPT_ENCODING, "");

        byte[] data = null;

        try {
            HttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();

            data = EntityUtils.toByteArray(entity);

        } finally {
            if (client instanceof Closeable) {
                Closeable t = (Closeable) client;
                t.close();
            }
        }

        if (chunkView.isGzipped) {
            data = Gzip.decompress(data);
        }

        if (chunkView.cipherKey != null && chunkView.cipherKey.length != 0) {
            try {
                data = SeaweedCipher.decrypt(data, chunkView.cipherKey);
            } catch (Exception e) {
                throw new IOException("fail to decrypt", e);
            }
        }

        return data;

    }

    protected static List<ChunkView> viewFromVisibles(List<VisibleInterval> visibleIntervals, long offset, long size) {
        List<ChunkView> views = new ArrayList<>();

        long stop = offset + size;
        for (VisibleInterval chunk : visibleIntervals) {
            if (chunk.start <= offset && offset < chunk.stop && offset < stop) {
                boolean isFullChunk = chunk.isFullChunk && chunk.start == offset && chunk.stop <= stop;
                views.add(new ChunkView(
                        chunk.fileId,
                        offset - chunk.start,
                        Math.min(chunk.stop, stop) - offset,
                        offset,
                        isFullChunk,
                        chunk.cipherKey,
                        chunk.isGzipped
                ));
                offset = Math.min(chunk.stop, stop);
            }
        }
        return views;
    }

    public static List<VisibleInterval> nonOverlappingVisibleIntervals(List<FilerProto.FileChunk> chunkList) {
        FilerProto.FileChunk[] chunks = chunkList.toArray(new FilerProto.FileChunk[0]);
        Arrays.sort(chunks, new Comparator<FilerProto.FileChunk>() {
            @Override
            public int compare(FilerProto.FileChunk a, FilerProto.FileChunk b) {
                return (int) (a.getMtime() - b.getMtime());
            }
        });

        List<VisibleInterval> visibles = new ArrayList<>();
        for (FilerProto.FileChunk chunk : chunks) {
            List<VisibleInterval> newVisibles = new ArrayList<>();
            visibles = mergeIntoVisibles(visibles, newVisibles, chunk);
        }

        return visibles;
    }

    private static List<VisibleInterval> mergeIntoVisibles(List<VisibleInterval> visibles,
                                                           List<VisibleInterval> newVisibles,
                                                           FilerProto.FileChunk chunk) {
        VisibleInterval newV = new VisibleInterval(
                chunk.getOffset(),
                chunk.getOffset() + chunk.getSize(),
                chunk.getFileId(),
                chunk.getMtime(),
                true,
                chunk.getCipherKey().toByteArray(),
                chunk.getIsGzipped()
        );

        // easy cases to speed up
        if (visibles.size() == 0) {
            visibles.add(newV);
            return visibles;
        }
        if (visibles.get(visibles.size() - 1).stop <= chunk.getOffset()) {
            visibles.add(newV);
            return visibles;
        }

        for (VisibleInterval v : visibles) {
            if (v.start < chunk.getOffset() && chunk.getOffset() < v.stop) {
                newVisibles.add(new VisibleInterval(
                        v.start,
                        chunk.getOffset(),
                        v.fileId,
                        v.modifiedTime,
                        false,
                        v.cipherKey,
                        v.isGzipped
                ));
            }
            long chunkStop = chunk.getOffset() + chunk.getSize();
            if (v.start < chunkStop && chunkStop < v.stop) {
                newVisibles.add(new VisibleInterval(
                        chunkStop,
                        v.stop,
                        v.fileId,
                        v.modifiedTime,
                        false,
                        v.cipherKey,
                        v.isGzipped
                ));
            }
            if (chunkStop <= v.start || v.stop <= chunk.getOffset()) {
                newVisibles.add(v);
            }
        }
        newVisibles.add(newV);

        // keep everything sorted
        for (int i = newVisibles.size() - 1; i >= 0; i--) {
            if (i > 0 && newV.start < newVisibles.get(i - 1).start) {
                newVisibles.set(i, newVisibles.get(i - 1));
            } else {
                newVisibles.set(i, newV);
                break;
            }
        }

        return newVisibles;
    }

    public static String parseVolumeId(String fileId) {
        int commaIndex = fileId.lastIndexOf(',');
        if (commaIndex > 0) {
            return fileId.substring(0, commaIndex);
        }
        return fileId;
    }

    public static long totalSize(List<FilerProto.FileChunk> chunksList) {
        long size = 0;
        for (FilerProto.FileChunk chunk : chunksList) {
            long t = chunk.getOffset() + chunk.getSize();
            if (size < t) {
                size = t;
            }
        }
        return size;
    }

    public static class VisibleInterval {
        public final long start;
        public final long stop;
        public final long modifiedTime;
        public final String fileId;
        public final boolean isFullChunk;
        public final byte[] cipherKey;
        public final boolean isGzipped;

        public VisibleInterval(long start, long stop, String fileId, long modifiedTime, boolean isFullChunk, byte[] cipherKey, boolean isGzipped) {
            this.start = start;
            this.stop = stop;
            this.modifiedTime = modifiedTime;
            this.fileId = fileId;
            this.isFullChunk = isFullChunk;
            this.cipherKey = cipherKey;
            this.isGzipped = isGzipped;
        }

        @Override
        public String toString() {
            return "VisibleInterval{" +
                    "start=" + start +
                    ", stop=" + stop +
                    ", modifiedTime=" + modifiedTime +
                    ", fileId='" + fileId + '\'' +
                    ", isFullChunk=" + isFullChunk +
                    ", cipherKey=" + Arrays.toString(cipherKey) +
                    ", isGzipped=" + isGzipped +
                    '}';
        }
    }

    public static class ChunkView {
        public final String fileId;
        public final long offset;
        public final long size;
        public final long logicOffset;
        public final boolean isFullChunk;
        public final byte[] cipherKey;
        public final boolean isGzipped;

        public ChunkView(String fileId, long offset, long size, long logicOffset, boolean isFullChunk, byte[] cipherKey, boolean isGzipped) {
            this.fileId = fileId;
            this.offset = offset;
            this.size = size;
            this.logicOffset = logicOffset;
            this.isFullChunk = isFullChunk;
            this.cipherKey = cipherKey;
            this.isGzipped = isGzipped;
        }

        @Override
        public String toString() {
            return "ChunkView{" +
                    "fileId='" + fileId + '\'' +
                    ", offset=" + offset +
                    ", size=" + size +
                    ", logicOffset=" + logicOffset +
                    ", isFullChunk=" + isFullChunk +
                    ", cipherKey=" + Arrays.toString(cipherKey) +
                    ", isGzipped=" + isGzipped +
                    '}';
        }
    }

}
