package seaweed.hdfs;

// adapted from org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream

import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedOutputStream;

public class SeaweedHadoopOutputStream extends SeaweedOutputStream {

    public SeaweedHadoopOutputStream(FilerClient filerClient, final String path, FilerProto.Entry.Builder entry,
                                     final long position, final int bufferSize, final String replication) {
        super(filerClient, path, entry, position, bufferSize, replication);
    }

}
