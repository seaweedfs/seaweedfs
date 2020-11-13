package com.example.test;

import seaweed.hdfs.SeaweedInputStream;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerGrpcClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Example {

    public static FilerClient filerClient = new FilerClient("localhost", 18888);
    public static FilerGrpcClient filerGrpcClient = new FilerGrpcClient("localhost", 18888);

    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        // 本地模式，速度很快
        parseZip("/Users/chris/tmp/test.zip");

        long startTime2 = System.currentTimeMillis();

        long localProcessTime = startTime2 - startTime;

        // swfs读取，慢
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
