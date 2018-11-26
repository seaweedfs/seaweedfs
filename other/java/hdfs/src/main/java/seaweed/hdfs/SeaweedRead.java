package seaweed.hdfs;

import seaweedfs.client.FilerProto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SeaweedRead {

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

}
