package seaweedfs.client;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FileChunkManifestTest {

    private static FilerProto.FileChunk chunk(int volumeId, long key, long offset, long size) {
        return FilerProto.FileChunk.newBuilder()
                .setFid(FilerProto.FileId.newBuilder().setVolumeId(volumeId).setFileKey(key).setCookie(1))
                .setOffset(offset)
                .setSize(size)
                .setModifiedTsNs(1)
                .build();
    }

    @Test
    public void testCutToManifestSpanCutsAShortenedManifest() throws IOException {
        List<FilerProto.FileChunk> held = Arrays.asList(chunk(9, 1, 0, 100), chunk(9, 2, 100, 100), chunk(9, 3, 200, 100));
        FilerProto.FileChunk manifest = chunk(9, 4, 0, 150).toBuilder().setIsChunkManifest(true).build();

        List<FilerProto.FileChunk> cut = FileChunkManifest.cutToManifestSpan(manifest, held);
        Assert.assertEquals(2, cut.size());
        Assert.assertEquals(held.get(0), cut.get(0));
        Assert.assertEquals(100, cut.get(1).getOffset());
        Assert.assertEquals(50, cut.get(1).getSize());
        Assert.assertEquals(100, held.get(1).getSize());

        List<SeaweedRead.VisibleInterval> visibles = ReadChunks.readResolvedChunks(cut);
        Assert.assertEquals(150, visibles.get(visibles.size() - 1).stop);
    }

    @Test
    public void testCutToManifestSpanLeavesAWholeManifestAlone() {
        List<FilerProto.FileChunk> held = Arrays.asList(chunk(9, 1, 0, 100), chunk(9, 2, 100, 100));
        FilerProto.FileChunk manifest = chunk(9, 4, 0, 200).toBuilder().setIsChunkManifest(true).build();
        Assert.assertEquals(held, FileChunkManifest.cutToManifestSpan(manifest, held));
    }
}
