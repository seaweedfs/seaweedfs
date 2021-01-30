package seaweed.hdfs;

// adapted from org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream

import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedOutputStream;

public class SeaweedHadoopOutputStream extends SeaweedOutputStream {

    public SeaweedHadoopOutputStream(FilerGrpcClient filerGrpcClient, final String path, FilerProto.Entry.Builder entry,
                                     final long position, final int bufferSize, final String replication) {
        super(filerGrpcClient, path.toString(), entry, position, bufferSize, replication);
    }

}
