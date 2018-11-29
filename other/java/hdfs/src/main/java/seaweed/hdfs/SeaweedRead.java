package seaweed.hdfs;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SeaweedRead {

    // returns bytesRead
    public static long read(FilerGrpcClient filerGrpcClient, List<VisibleInterval> visibleIntervals,
                            final long position, final byte[] buffer, final int bufferOffset,
                            final int bufferLength) {

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
        for (ChunkView chunkView : chunkViews) {
            FilerProto.Locations locations = vid2Locations.get(parseVolumeId(chunkView.fileId));
            if (locations.getLocationsCount() == 0) {
                // log here!
                return 0;
            }

            HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(
                String.format("http://%s/%s", locations.getLocations(0).getUrl(), chunkView.fileId));
            request.setHeader("Range",
                String.format("bytes=%d-%d", chunkView.offset, chunkView.offset + chunkView.size));

            try {
                HttpResponse response = client.execute(request);
                HttpEntity entity = response.getEntity();

                readCount += entity.getContent().read(buffer,
                    (int) (chunkView.logicOffset - position),
                    (int) (chunkView.logicOffset - position + chunkView.size));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return readCount;
    }

    private static List<ChunkView> viewFromVisibles(List<VisibleInterval> visibleIntervals, long offset, long size) {
        List<ChunkView> views = new ArrayList<>();

        long stop = offset + size;
        for (VisibleInterval chunk : visibleIntervals) {
            views.add(new ChunkView(
                chunk.fileId,
                offset - chunk.start,
                Math.min(chunk.stop, stop) - offset,
                offset
            ));
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

        List<VisibleInterval> newVisibles = new ArrayList<>();
        List<VisibleInterval> visibles = new ArrayList<>();
        for (FilerProto.FileChunk chunk : chunks) {
            newVisibles = mergeIntoVisibles(visibles, newVisibles, chunk);
            visibles.clear();
            List<VisibleInterval> t = visibles;
            visibles = newVisibles;
            newVisibles = t;
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
            chunk.getMtime()
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
                    v.modifiedTime
                ));
            }
            long chunkStop = chunk.getOffset() + chunk.getSize();
            if (v.start < chunkStop && chunkStop < v.stop) {
                newVisibles.add(new VisibleInterval(
                    chunkStop,
                    v.stop,
                    v.fileId,
                    v.modifiedTime
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

    public static class VisibleInterval {
        long start;
        long stop;
        long modifiedTime;
        String fileId;

        public VisibleInterval(long start, long stop, String fileId, long modifiedTime) {
            this.start = start;
            this.stop = stop;
            this.modifiedTime = modifiedTime;
            this.fileId = fileId;
        }
    }

    public static class ChunkView {
        String fileId;
        long offset;
        long size;
        long logicOffset;

        public ChunkView(String fileId, long offset, long size, long logicOffset) {
            this.fileId = fileId;
            this.offset = offset;
            this.size = size;
            this.logicOffset = logicOffset;
        }
    }

}
