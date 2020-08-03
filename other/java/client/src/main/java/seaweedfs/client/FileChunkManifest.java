package seaweedfs.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileChunkManifest {

    private static final Logger LOG = LoggerFactory.getLogger(FileChunkManifest.class);

    private static final int mergeFactor = 1000;

    public static boolean hasChunkManifest(List<FilerProto.FileChunk> chunks) {
        for (FilerProto.FileChunk chunk : chunks) {
            if (chunk.getIsChunkManifest()) {
                return true;
            }
        }
        return false;
    }

    public static List<FilerProto.FileChunk> resolveChunkManifest(
            final FilerGrpcClient filerGrpcClient, List<FilerProto.FileChunk> chunks) throws IOException {

        List<FilerProto.FileChunk> dataChunks = new ArrayList<>();

        for (FilerProto.FileChunk chunk : chunks) {
            if (!chunk.getIsChunkManifest()) {
                dataChunks.add(chunk);
                continue;
            }

            // IsChunkManifest
            LOG.debug("fetching chunk manifest:{}", chunk);
            byte[] data = fetchChunk(filerGrpcClient, chunk);
            FilerProto.FileChunkManifest m = FilerProto.FileChunkManifest.newBuilder().mergeFrom(data).build();
            List<FilerProto.FileChunk> resolvedChunks = new ArrayList<>();
            for (FilerProto.FileChunk t : m.getChunksList()) {
                // avoid deprecated chunk.getFileId()
                resolvedChunks.add(t.toBuilder().setFileId(FilerClient.toFileId(t.getFid())).build());
            }
            dataChunks.addAll(resolveChunkManifest(filerGrpcClient, resolvedChunks));
        }

        return dataChunks;
    }

    private static byte[] fetchChunk(final FilerGrpcClient filerGrpcClient, FilerProto.FileChunk chunk) throws IOException {

        String vid = "" + chunk.getFid().getVolumeId();
        FilerProto.Locations locations = filerGrpcClient.vidLocations.get(vid);
        if (locations == null) {
            FilerProto.LookupVolumeRequest.Builder lookupRequest = FilerProto.LookupVolumeRequest.newBuilder();
            lookupRequest.addVolumeIds(vid);
            FilerProto.LookupVolumeResponse lookupResponse = filerGrpcClient
                    .getBlockingStub().lookupVolume(lookupRequest.build());
            locations = lookupResponse.getLocationsMapMap().get(vid);
            filerGrpcClient.vidLocations.put(vid, locations);
            LOG.debug("fetchChunk vid:{} locations:{}", vid, locations);
        }

        SeaweedRead.ChunkView chunkView = new SeaweedRead.ChunkView(
                FilerClient.toFileId(chunk.getFid()), // avoid deprecated chunk.getFileId()
                0,
                -1,
                0,
                true,
                chunk.getCipherKey().toByteArray(),
                chunk.getIsCompressed());

        byte[] chunkData = SeaweedRead.chunkCache.getChunk(chunkView.fileId);
        if (chunkData == null) {
            LOG.debug("doFetchFullChunkData:{}", chunkView);
            chunkData = SeaweedRead.doFetchFullChunkData(chunkView, locations);
        }
        if（chunk.getIsChunkManifest()){
            // only cache manifest chunks
            LOG.debug("chunk {} size {}", chunkView.fileId, chunkData.length);
            SeaweedRead.chunkCache.setChunk(chunkView.fileId, chunkData);
        }

        return chunkData;

    }

    public static List<FilerProto.FileChunk> maybeManifestize(
            final FilerGrpcClient filerGrpcClient, List<FilerProto.FileChunk> inputChunks) throws IOException {
        // the return variable
        List<FilerProto.FileChunk> chunks = new ArrayList<>();

        List<FilerProto.FileChunk> dataChunks = new ArrayList<>();
        for (FilerProto.FileChunk chunk : inputChunks) {
            if (!chunk.getIsChunkManifest()) {
                dataChunks.add(chunk);
            } else {
                chunks.add(chunk);
            }
        }

        int remaining = dataChunks.size();
        for (int i = 0; i + mergeFactor < dataChunks.size(); i += mergeFactor) {
            FilerProto.FileChunk chunk = mergeIntoManifest(filerGrpcClient, dataChunks.subList(i, i + mergeFactor));
            chunks.add(chunk);
            remaining -= mergeFactor;
        }

        // remaining
        for (int i = dataChunks.size() - remaining; i < dataChunks.size(); i++) {
            chunks.add(dataChunks.get(i));
        }
        return chunks;
    }

    private static FilerProto.FileChunk mergeIntoManifest(final FilerGrpcClient filerGrpcClient, List<FilerProto.FileChunk> dataChunks) throws IOException {
        // create and serialize the manifest
        dataChunks = FilerClient.beforeEntrySerialization(dataChunks);
        FilerProto.FileChunkManifest.Builder m = FilerProto.FileChunkManifest.newBuilder().addAllChunks(dataChunks);
        byte[] data = m.build().toByteArray();

        long minOffset = Long.MAX_VALUE;
        long maxOffset = -1;
        for (FilerProto.FileChunk chunk : dataChunks) {
            minOffset = Math.min(minOffset, chunk.getOffset());
            maxOffset = Math.max(maxOffset, chunk.getSize() + chunk.getOffset());
        }

        FilerProto.FileChunk.Builder manifestChunk = SeaweedWrite.writeChunk(
                filerGrpcClient.getReplication(),
                filerGrpcClient,
                minOffset,
                data, 0, data.length);
        manifestChunk.setIsChunkManifest(true);
        manifestChunk.setSize(maxOffset - minOffset);
        return manifestChunk.build();

    }

}
