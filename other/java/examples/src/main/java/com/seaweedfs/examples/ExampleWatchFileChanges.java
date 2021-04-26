package com.seaweedfs.examples;

import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerProto;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public class ExampleWatchFileChanges {

    public static void main(String[] args) throws IOException {

        FilerClient filerClient = new FilerClient("localhost", 18888);

        long sinceNs = (System.currentTimeMillis() - 3600 * 1000) * 1000000L;

        Iterator<FilerProto.SubscribeMetadataResponse> watch = filerClient.watch(
                "/buckets",
                "exampleClientName",
                sinceNs
        );

        System.out.println("Connected to filer, subscribing from " + new Date());

        while (watch.hasNext()) {
            FilerProto.SubscribeMetadataResponse event = watch.next();
            FilerProto.EventNotification notification = event.getEventNotification();
            if (!event.getDirectory().equals(notification.getNewParentPath())) {
                // move an entry to a new directory, possibly with a new name
                if (notification.hasOldEntry() && notification.hasNewEntry()) {
                    System.out.println("moved " + event.getDirectory() + "/" + notification.getOldEntry().getName() + " to " + notification.getNewParentPath() + "/" + notification.getNewEntry().getName());
                } else {
                    System.out.println("this should not happen.");
                }
            } else if (notification.hasNewEntry() && !notification.hasOldEntry()) {
                System.out.println("created entry " + event.getDirectory() + "/" + notification.getNewEntry().getName());
            } else if (!notification.hasNewEntry() && notification.hasOldEntry()) {
                System.out.println("deleted entry " + event.getDirectory() + "/" + notification.getOldEntry().getName());
            } else if (notification.hasNewEntry() && notification.hasOldEntry()) {
                System.out.println("updated entry " + event.getDirectory() + "/" + notification.getNewEntry().getName());
            }
        }

    }
}
