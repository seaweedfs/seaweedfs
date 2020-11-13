package com.seaweedfs.examples;

import seaweed.hdfs.SeaweedInputStream;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerGrpcClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnzipFile {

    public static void main(String[] args) throws IOException {

        FilerGrpcClient filerGrpcClient = new FilerGrpcClient("localhost", 18888);
        FilerClient filerClient = new FilerClient(filerGrpcClient);

        long startTime = System.currentTimeMillis();
        parseZip("/Users/chris/tmp/test.zip");

        long startTime2 = System.currentTimeMillis();

        long localProcessTime = startTime2 - startTime;

        SeaweedInputStream seaweedInputStream = new SeaweedInputStream(
                filerGrpcClient,
                new org.apache.hadoop.fs.FileSystem.Statistics(""),
                "/",
                filerClient.lookupEntry("/", "test.zip")
        );
        parseZip(seaweedInputStream);

        long swProcessTime = System.currentTimeMillis() - startTime2;

        System.out.println("Local time: " + localProcessTime);
        System.out.println("SeaweedFS time: " + swProcessTime);

    }

    public static void parseZip(String filename) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        parseZip(fileInputStream);
    }

    public static void parseZip(InputStream is) throws IOException {
        ZipInputStream zin = new ZipInputStream(is);
        ZipEntry ze;
        while ((ze = zin.getNextEntry()) != null) {
            System.out.println(ze.getName());
        }
    }
}
