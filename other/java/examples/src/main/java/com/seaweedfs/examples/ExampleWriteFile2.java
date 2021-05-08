package com.seaweedfs.examples;

import com.google.common.io.Files;
import seaweedfs.client.FilerClient;
import seaweedfs.client.SeaweedOutputStream;

import java.io.File;
import java.io.IOException;

public class ExampleWriteFile2 {

    public static void main(String[] args) throws IOException {

        FilerClient filerClient = new FilerClient("localhost", 18888);

        SeaweedOutputStream seaweedOutputStream = new SeaweedOutputStream(filerClient, "/test/1");
        Files.copy(new File("/etc/resolv.conf"), seaweedOutputStream);
        seaweedOutputStream.close();

    }

}
