package seaweedfs.client;

import java.io.IOException;

public class RemoteUtil {
    public static boolean isInRemoteOnly(FilerProto.Entry entry) {
        if (entry.getChunksList() == null || entry.getChunksList().isEmpty()) {
            return entry.getRemoteEntry() != null && entry.getRemoteEntry().getRemoteSize() > 0;
        }
        return false;
    }

    public static FilerProto.Entry downloadRemoteEntry(FilerClient filerClient, String fullpath, FilerProto.Entry entry) throws IOException {
        String dir = SeaweedOutputStream.getParentDirectory(fullpath);
        String name = SeaweedOutputStream.getFileName(fullpath);

        final FilerProto.DownloadToLocalResponse downloadToLocalResponse = filerClient.getBlockingStub()
                .downloadToLocal(FilerProto.DownloadToLocalRequest.newBuilder()
                .setDirectory(dir).setName(name).build());

        return downloadToLocalResponse.getEntry();
    }
}
