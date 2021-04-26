How replication works
======

All metadata changes within current cluster would be notified to a message queue.

If the meta data change is from other clusters, this metadata would change would not be notified to the message queue.

So active<=>active replication is possible.


All metadata changes would be published as metadata changes.
So all mounts listening for metadata changes will get updated.