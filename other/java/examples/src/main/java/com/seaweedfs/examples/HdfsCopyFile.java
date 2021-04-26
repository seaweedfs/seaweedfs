package com.seaweedfs.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class HdfsCopyFile {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS", "seaweedfs://localhost:8888");
        configuration.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");

        FileSystem fs = FileSystem.get(configuration);
        String source = "/Users/chris/tmp/test.zip";
        String destination = "/buckets/spark/test01.zip";
        InputStream in = new BufferedInputStream(new FileInputStream(source));

        OutputStream out = fs.create(new Path(destination));
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
