package seaweedfs.client;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class SeaweedRead {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedRead.class);

    static ChunkCache chunkCache = new ChunkCache(4);
    static VolumeIdCache volumeIdCache = new VolumeIdCache(4 * 1024);

    // returns bytesRead
    public static long read(FilerClient filerClient, List<VisibleInterval> visibleIntervals,
                            final long position, final ByteBuffer buf, final long fileSize) throws IOException {

        List<ChunkView> chunkViews = viewFromVisibles(visibleIntervals, position, buf.remaining());

        Map<String, FilerProto.Locations> knownLocations = new HashMap<>();

        FilerProto.LookupVolumeRequest.Builder lookupRequest = FilerProto.LookupVolumeRequest.newBuilder();
        for (ChunkView chunkView : chunkViews) {
            String vid = parseVolumeId(chunkView.fileId);
            FilerProto.Locations locations = volumeIdCache.getLocations(vid);
            if (locations == null) {
                lookupRequest.addVolumeIds(vid);
            } else {
                knownLocations.put(vid, locations);
            }
        }

        if (lookupRequest.getVolumeIdsCount() > 0) {
            FilerProto.LookupVolumeResponse lookupResponse = filerClient
                    .getBlockingStub().lookupVolume(lookupRequest.build());
            Map<String, FilerProto.Locations> vid2Locations = lookupResponse.getLocationsMapMap();
            for (Map.Entry<String, FilerProto.Locations> entry : vid2Locations.entrySet()) {
                volumeIdCache.setLocations(entry.getKey(), entry.getValue());
                knownLocations.put(entry.getKey(), entry.getValue());
            }
        }

        //TODO parallel this
        long readCount = 0;
        long startOffset = position;
        for (ChunkView chunkView : chunkViews) {

            if (startOffset < chunkView.logicOffset) {
                long gap = chunkView.logicOffset - startOffset;
                LOG.debug("zero [{},{})", startOffset, startOffset + gap);
                buf.position(buf.position()+ (int)gap);
                readCount += gap;
                startOffset += gap;
            }

            String volumeId = parseVolumeId(chunkView.fileId);
            FilerProto.Locations locations = knownLocations.get(volumeId);
            if (locations == null || locations.getLocationsCount() == 0) {
                LOG.error("failed to locate {}", chunkView.fileId);
                volumeIdCache.clearLocations(volumeId);
                throw new IOException("failed to locate fileId " + chunkView.fileId);
            }

            int len = readChunkView(filerClient, startOffset, buf, chunkView, locations);

            LOG.debug("read [{},{}) {} size {}", startOffset, startOffset + len, chunkView.fileId, chunkView.size);

            readCount += len;
            startOffset += len;

        }

        long limit = Math.min(buf.limit(), fileSize);

        if (startOffset < limit) {
            long gap = limit - startOffset;
            LOG.debug("zero2 [{},{})", startOffset, startOffset + gap);
            buf.position(buf.position()+ (int)gap);
            readCount += gap;
            startOffset += gap;
        }

        return readCount;
    }

    private static int readChunkView(FilerClient filerClient, long startOffset, ByteBuffer buf, ChunkView chunkView, FilerProto.Locations locations) throws IOException {

        byte[] chunkData = chunkCache.getChunk(chunkView.fileId);

        if (chunkData == null) {
            chunkData = doFetchFullChunkData(filerClient, chunkView, locations);
            chunkCache.setChunk(chunkView.fileId, chunkData);
        }

        int len = (int) chunkView.size - (int) (startOffset - chunkView.logicOffset);
        LOG.debug("readChunkView fid:{} chunkData.length:{} chunkView.offset:{} chunkView[{};{}) startOffset:{}",
                chunkView.fileId, chunkData.length, chunkView.offset, chunkView.logicOffset, chunkView.logicOffset + chunkView.size, startOffset);
        buf.put(chunkData, (int) (startOffset - chunkView.logicOffset + chunkView.offset), len);

        return len;
    }

    public static byte[] doFetchFullChunkData(FilerClient filerClient, ChunkView chunkView, FilerProto.Locations locations) throws IOException {

        byte[] data = null;
        IOException lastException = null;
        for (long waitTime = 1000L; waitTime < 10 * 1000; waitTime += waitTime / 2) {
            for (FilerProto.Location location : locations.getLocationsList()) {
                String url = filerClient.getChunkUrl(chunkView.fileId, location.getUrl(), location.getPublicUrl());
                try {
                    data = doFetchOneFullChunkData(chunkView, url);
                    lastException = null;
                    break;
                } catch (IOException ioe) {
                    LOG.debug("doFetchFullChunkData {} :{}", url, ioe);
                    lastException = ioe;
                }
            }
            if (data != null) {
                break;
            }
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
            }
        }

        if (lastException != null) {
            throw lastException;
        }

        LOG.debug("doFetchFullChunkData fid:{} chunkData.length:{}", chunkView.fileId, data.length);

        return data;

    }

    private static byte[] doFetchOneFullChunkData(ChunkView chunkView, String url) throws IOException {

        HttpGet request = new HttpGet(url);

        request.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

        byte[] data = null;

        CloseableHttpResponse response = SeaweedUtil.getClosableHttpClient().execute(request);

        try {
            HttpEntity entity = response.getEntity();

            Header contentEncodingHeader = entity.getContentEncoding();

            if (contentEncodingHeader != null) {
                HeaderElement[] encodings = contentEncodingHeader.getElements();
                for (int i = 0; i < encodings.length; i++) {
                    if (encodings[i].getName().equalsIgnoreCase("gzip")) {
                        entity = new GzipDecompressingEntity(entity);
                        break;
                    }
                }
            }

            data = EntityUtils.toByteArray(entity);

            EntityUtils.consume(entity);

        } finally {
            response.close();
            request.releaseConnection();
        }

        if (chunkView.cipherKey != null && chunkView.cipherKey.length != 0) {
            try {
                data = SeaweedCipher.decrypt(data, chunkView.cipherKey);
            } catch (Exception e) {
                throw new IOException("fail to decrypt", e);
            }
        }

        if (chunkView.isCompressed) {
            data = Gzip.decompress(data);
        }

        LOG.debug("doFetchOneFullChunkData url:{} chunkData.length:{}", url, data.length);

        return data;

    }

    protected static List<ChunkView> viewFromVisibles(List<VisibleInterval> visibleIntervals, long offset, long size) {
        List<ChunkView> views = new ArrayList<>();

        long stop = offset + size;
        for (VisibleInterval chunk : visibleIntervals) {
            long chunkStart = Math.max(offset, chunk.start);
            long chunkStop = Math.min(stop, chunk.stop);
            if (chunkStart < chunkStop) {
                boolean isFullChunk = chunk.isFullChunk && chunk.start == offset && chunk.stop <= stop;
                views.add(new ChunkView(
                        chunk.fileId,
                        chunkStart - chunk.start + chunk.chunkOffset,
                        chunkStop - chunkStart,
                        chunkStart,
                        isFullChunk,
                        chunk.cipherKey,
                        chunk.isCompressed
                ));
            }
        }
        return views;
    }

    public static List<VisibleInterval> nonOverlappingVisibleIntervals(
            final FilerClient filerClient, List<FilerProto.FileChunk> chunkList) throws IOException {

        chunkList = FileChunkManifest.resolveChunkManifest(filerClient, chunkList);

        return ReadChunks.readResolvedChunks(chunkList);

    }

    public static String parseVolumeId(String fileId) {
        int commaIndex = fileId.lastIndexOf(',');
        if (commaIndex > 0) {
            return fileId.substring(0, commaIndex);
        }
        return fileId;
    }

    public static long fileSize(FilerProto.Entry entry) {
        return Math.max(totalSize(entry.getChunksList()), entry.getAttributes().getFileSize());
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
        public final long chunkOffset;
        public final boolean isFullChunk;
        public final byte[] cipherKey;
        public final boolean isCompressed;

        public VisibleInterval(long start, long stop, String fileId, long modifiedTime, long chunkOffset, boolean isFullChunk, byte[] cipherKey, boolean isCompressed) {
            this.start = start;
            this.stop = stop;
            this.modifiedTime = modifiedTime;
            this.fileId = fileId;
            this.chunkOffset = chunkOffset;
            this.isFullChunk = isFullChunk;
            this.cipherKey = cipherKey;
            this.isCompressed = isCompressed;
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
                    ", isCompressed=" + isCompressed +
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
        public final boolean isCompressed;

        public ChunkView(String fileId, long offset, long size, long logicOffset, boolean isFullChunk, byte[] cipherKey, boolean isCompressed) {
            this.fileId = fileId;
            this.offset = offset;
            this.size = size;
            this.logicOffset = logicOffset;
            this.isFullChunk = isFullChunk;
            this.cipherKey = cipherKey;
            this.isCompressed = isCompressed;
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
                    ", isCompressed=" + isCompressed +
                    '}';
        }
    }

}
