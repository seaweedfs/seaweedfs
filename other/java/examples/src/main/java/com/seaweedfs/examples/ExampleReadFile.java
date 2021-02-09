package com.seaweedfs.examples;

import seaweedfs.client.FilerClient;
import seaweedfs.client.SeaweedInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ExampleReadFile {

    public static void main(String[] args) throws IOException {

        FilerClient filerClient = new FilerClient("localhost", 18888);

        long startTime = System.currentTimeMillis();
        parseZip("/Users/chris/tmp/test.zip");

        long startTime2 = System.currentTimeMillis();

        long localProcessTime = startTime2 - startTime;

        SeaweedInputStream seaweedInputStream = new SeaweedInputStream(
                filerClient, "/test.zip");
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
