package seaweedfs.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ReadChunks {

    public static List<SeaweedRead.VisibleInterval> readResolvedChunks(List<FilerProto.FileChunk> chunkList) throws IOException {
        List<Point> points = new ArrayList<>(chunkList.size() * 2);
        for (FilerProto.FileChunk chunk : chunkList) {
            points.add(new Point(chunk.getOffset(), chunk, true));
            points.add(new Point(chunk.getOffset() + chunk.getSize(), chunk, false));
        }

        Collections.sort(points, new Comparator<Point>() {
            @Override
            public int compare(Point a, Point b) {
                int xComparison = Long.compare(a.x, b.x);
                if (xComparison != 0) {
                    return xComparison;
                }

                // If x values are equal, compare ts
                int tsComparison = Long.compare(a.ts, b.ts);
                if (tsComparison != 0) {
                    return tsComparison;
                }

                // If both x and ts are equal, prioritize start points
                return Boolean.compare(b.isStart, a.isStart); // b.isStart first to prioritize starts
            }
        });

        long prevX = 0;
        List<SeaweedRead.VisibleInterval> visibles = new ArrayList<>();
        ArrayList<Point> queue = new ArrayList<>();
        for (Point point : points) {
            if (point.isStart) {
                if (queue.size() > 0) {
                    int lastIndex = queue.size() - 1;
                    Point lastPoint = queue.get(lastIndex);
                    if (point.x != prevX && lastPoint.ts < point.ts) {
                        addToVisibles(visibles, prevX, lastPoint, point);
                        prevX = point.x;
                    }
                }
                // insert into queue
                for (int i = queue.size(); i >= 0; i--) {
                    if (i == 0 || queue.get(i - 1).ts <= point.ts) {
                        if (i == queue.size()) {
                            prevX = point.x;
                        }
                        queue.add(i, point);
                        break;
                    }
                }
            } else {
                int lastIndex = queue.size() - 1;
                int index = lastIndex;
                Point startPoint = null;
                for (; index >= 0; index--) {
                    startPoint = queue.get(index);
                    if (startPoint.ts == point.ts) {
                        queue.remove(index);
                        break;
                    }
                }
                if (index == lastIndex && startPoint != null) {
                    addToVisibles(visibles, prevX, startPoint, point);
                    prevX = point.x;
                }
            }
        }

        return visibles;

    }

    private static void addToVisibles(List<SeaweedRead.VisibleInterval> visibles, long prevX, Point startPoint, Point point) {
        if (prevX < point.x) {
            FilerProto.FileChunk chunk = startPoint.chunk;
            visibles.add(new SeaweedRead.VisibleInterval(
                    prevX,
                    point.x,
                    chunk.getFileId(),
                    chunk.getModifiedTsNs(),
                    prevX - chunk.getOffset(),
                    chunk.getOffset() == prevX && chunk.getSize() == prevX - startPoint.x,
                    chunk.getCipherKey().toByteArray(),
                    chunk.getIsCompressed()
            ));
        }
    }

    static class Point {
        long x;
        long ts;
        FilerProto.FileChunk chunk;
        boolean isStart;

        public Point(long x, FilerProto.FileChunk chunk, boolean isStart) {
            this.x = x;
            this.ts = chunk.getModifiedTsNs();
            this.chunk = chunk;
            this.isStart = isStart;
        }
    }

}
