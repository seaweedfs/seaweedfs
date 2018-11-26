package seaweed.hdfs;

import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.util.List;

public class SeaweedWrite {
    public static FilerProto.Entry writeData(final FilerGrpcClient filerGrpcClient, final long offset,
                                             final byte[] bytes, final long bytesOffset, final long bytesLength) {
        return null;
    }

    public static void writeMeta(final FilerGrpcClient filerGrpcClient,
                                 final String path, final List<FilerProto.Entry> entries) {
        return;
    }
}
