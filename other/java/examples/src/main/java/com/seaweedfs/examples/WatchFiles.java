package com.seaweedfs.examples;

import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.IOException;
import java.util.Iterator;

public class WatchFiles {

    public static void main(String[] args) throws IOException {
        FilerGrpcClient filerGrpcClient = new FilerGrpcClient("localhost", 18888);
        FilerClient filerClient = new FilerClient(filerGrpcClient);

        Iterator<FilerProto.SubscribeMetadataResponse> watch = filerClient.watch(
                "/buckets",
                "exampleClient",
                System.currentTimeMillis() * 1000000L
        );

        while (watch.hasNext()) {
            FilerProto.SubscribeMetadataResponse event = watch.next();
            FilerProto.EventNotification notification = event.getEventNotification();
            if (notification.getNewParentPath() != null) {
                // move an entry to a new directory, possibly with a new name
                if (notification.hasOldEntry() && notification.hasNewEntry()) {
                    System.out.println("move " + event.getDirectory() + "/" + notification.getOldEntry().getName() + " to " + notification.getNewParentPath() + "/" + notification.getNewEntry().getName());
                } else {
                    System.out.println("this should not happen.");
                }
            } else if (notification.hasNewEntry() && !notification.hasOldEntry()) {
                System.out.println("create entry " + event.getDirectory() + "/" + notification.getNewEntry().getName());
            } else if (!notification.hasNewEntry() && notification.hasOldEntry()) {
                System.out.println("delete entry " + event.getDirectory() + "/" + notification.getOldEntry().getName());
            } else if (notification.hasNewEntry() && notification.hasOldEntry()) {
                System.out.println("updated entry " + event.getDirectory() + "/" + notification.getNewEntry().getName());
            }
        }

    }
}
