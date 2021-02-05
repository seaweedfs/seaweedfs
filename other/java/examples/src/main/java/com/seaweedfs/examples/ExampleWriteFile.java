package com.seaweedfs.examples;

import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.SeaweedInputStream;
import seaweedfs.client.SeaweedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ExampleWriteFile {

    public static void main(String[] args) throws IOException {

        FilerGrpcClient filerGrpcClient = new FilerGrpcClient("localhost", 18888);

        SeaweedInputStream seaweedInputStream = new SeaweedInputStream(
                filerGrpcClient, "/test.zip");
        unZipFiles(filerGrpcClient, seaweedInputStream);

    }

    public static void unZipFiles(FilerGrpcClient filerGrpcClient, InputStream is) throws IOException {
        ZipInputStream zin = new ZipInputStream(is);
        ZipEntry ze;
        while ((ze = zin.getNextEntry()) != null) {

            String filename = ze.getName();
            if (filename.indexOf("/") >= 0) {
                filename = filename.substring(filename.lastIndexOf("/") + 1);
            }
            if (filename.length()==0) {
                continue;
            }

            SeaweedOutputStream seaweedOutputStream = new SeaweedOutputStream(filerGrpcClient, "/test/"+filename);
            byte[] bytesIn = new byte[16 * 1024];
            int read = 0;
            while ((read = zin.read(bytesIn))!=-1) {
                seaweedOutputStream.write(bytesIn,0,read);
            }
            seaweedOutputStream.close();

            System.out.println(ze.getName());
        }
    }
}
