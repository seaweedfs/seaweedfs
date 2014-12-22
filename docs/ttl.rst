Store file with a Time To Live (TTL).
===================

Introduction
#############################

Seaweed is a key~file store, and files can optionally expire with a Time To Live (TTL).

How to use it?
#############################

Assume we want to store a file with TTL of 3 minutes.

First, ask the master to assign a file id to a volume with a 3-minute TTL:
.. code-block:: bash
  > curl http://localhost:9333/dir/assign?ttl=3m
  {"count":1,"fid":"5,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}

Secondly, use the file id to store on the volume server
.. code-block:: bash
  > curl -F "file=@x.go" http://127.0.0.1:8080/5,01637037d6?ttl=3m

After writing, the file content will be returned as usual if read before the TTL expiry. But if read after the TTL expiry, the file will be reported as missing and return the http response status as not found.

For next writes with ttl=3m, the same set of volumes with ttl=3m will be used until:

1. the ttl=3m volumes are full. If so, new volumes will be created.
2. there are no write activities for 3 minutes. If so, these volumes will be stopped and deleted.

Advanced Usage
#############################

As you may have noticed, the "ttl=3m" is used twice! One for assigning file id, and one for uploading the actual file. The first one is for master to pick a matching volume, while the second one is written together with the file.

These two TTL values are not required to be the same. As long as the volume TTL is larger than file TTL, it should be OK.

This gives some flexibility to fine-tune the file TTL, while reducing the number of volume TTL variations, which simplifies managing the TTL volumes.

Supported TTL format
#############################

The TTL is in the format of one integer number followed by one unit. The unit can be 'm', 'h', 'd', 'w', 'M', 'y'.

Supported TTL format examples:

- 3m: 3 minutes
-  4h: 4 hours
-  5d: 5 days
-  6w: 6 weeks
-  7M: 7 months
-  8y: 8 years


How efficient it is?
#############################

TTL seems easy to implement since we just need to report the file as missing if the time is over the TTL. However, the real difficulty is to efficiently reclaim disk space from expired files, similar to JVM memory garbage collection, which is a sophisticated piece of work with many man-years of work.

Memcached also supports TTL. It gets around this problem by putting entries into fix-sized slabs. If one slab is expired, no work is required and the slab can be overwritten right away. However, this fix-sized slab approach wastes disk spaces since the file contents rarely fit in slabs exactly. And the slab managements are not trivial either.

Seaweed-FS efficiently resolves this disk space garbage collection problem with great simplicity. One of key differences from "normal" implementation is that the TTL is associated with the volume, together with each specific file.

During the file id assigning step, the file id will be assigned to a volume with matching TTL. The volumes are checked periodically (every 5~10 seconds by default). If the latest expiration time has been reached, all the files in the whole volume will be all expired, and the volume can be safely deleted.

Implementation Details
#############################
1. When assigning file key, the master would pick one TTL volume with matching TTL. If such volumes do not exist, create a few.
2. Volume servers will write the file with expiration time. When serving file, if the file is expired, the file will be reported as not found.
3. Volume servers will track each volume's largest expiration time, and stop reporting the expired volumes to the master server.
4. Master server will think the previously existed volumes are dead, and stop assigning write requests to them.
5. After about 10% of the TTL time, or at most 10 minutes, the volume servers will delete the expired volume.

Deployment
#############################

For deploying to production, the TTL volume maximum size should be taken into consideration. If the writes are frequent, the TTL volume will grow to the max volume size. So when the disk space is not ample enough, it's better to reduce the maximum volume size.

It's recommended not to mix the TTL volumes and non TTL volumes in the same cluster. This is because the volume maximum size, default to 30GB, is configured on the volume master at the cluster level.

We could implement the configuration for max volume size for each TTL. However, it could get fairly verbose. Maybe later if it is strongly desired.

