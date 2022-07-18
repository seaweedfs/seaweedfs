# SeaweedMQ Message Queue on SeaweedFS (WIP, not ready)

## What are the use cases it is designed for?

Message queues are like water pipes. Messages flow in the pipes to their destinations.

However, what if a flood comes? Of course, you can increase the number of partitions, add more brokers, restart, 
and watch the traffic level closely.

Sometimes the flood is expected. For example, backfill some old data in batch, and switch to online messages. 
You may want to ensure enough brokers to handle the data and reduce them later to cut cost.

SeaweedMQ is designed for use cases that need to:
* Receive and save large number of messages.
* Handle spike traffic automatically.

## What is special about SeaweedMQ?

* Separate computation and storage nodes that scales independently.
  * Offline messages can be operated as normal files.
  * Unlimited storage space by adding volume servers.
  * Unlimited message brokers.
* Scale up and down with auto split and merge message topics.
  * Topics can automatically split into segments when traffic increases, and vice verse.
  * 

# Design

# How it works?

Brokers are just computation nodes without storage. When a broker starts, it reports itself to masters.
Among all the brokers, one of them will be selected as the leader by the masters.

A topic needs to define its partition key on its messages.

Messages for a topic are divided into segments. 

During write time, the client will ask the broker leader for a few brokers to process the segment.

The broker leader will check whether the segment already has assigned the brokers. If not, select a few based
on their loads, save the selection into filer, and tell the client.

The client will write the messages for this segment to the selected brokers.

## Failover

The broker leader does not contain any state. If it fails, the masters will select a different broker.

For a segment, if any one of the selected brokers is down, the remaining brokers should try to write received messages
to the filer, and close the segment to the clients.

Then the clients should start a new segment. The masters should other healthy brokers to handle the new segment.

So any brokers can go down without losing data.

## Auto Split or Merge

(The idea is learned from Pravega.)

The brokers should report its traffic load to the broker leader periodically.

If any segment has too much load, the broker leader will ask the brokers to tell the client to 
close current one and create two new segments.

If 2 neighboring segments have the combined load below average load per segment, the broker leader will ask 
the brokers to tell the client to close this 2 segments and create a new segment.
