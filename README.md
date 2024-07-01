# SeaweedFS


[![Slack](https://img.shields.io/badge/slack-purple)](https://join.slack.com/t/seaweedfs/shared_invite/enQtMzI4MTMwMjU2MzA3LTEyYzZmZWYzOGQ3MDJlZWMzYmI0OTE4OTJiZjJjODBmMzUxNmYwODg0YjY3MTNlMjBmZDQ1NzQ5NDJhZWI2ZmY)
[![Twitter](https://img.shields.io/twitter/follow/seaweedfs.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=seaweedfs)
[![Build Status](https://img.shields.io/github/actions/workflow/status/seaweedfs/seaweedfs/go.yml)](https://github.com/seaweedfs/seaweedfs/actions/workflows/go.yml)
[![GoDoc](https://godoc.org/github.com/seaweedfs/seaweedfs/weed?status.svg)](https://godoc.org/github.com/seaweedfs/seaweedfs/weed)
[![Wiki](https://img.shields.io/badge/docs-wiki-blue.svg)](https://github.com/seaweedfs/seaweedfs/wiki)
[![Docker Pulls](https://img.shields.io/docker/pulls/chrislusf/seaweedfs?maxAge=4800)](https://hub.docker.com/r/chrislusf/seaweedfs/)
[![SeaweedFS on Maven Central](https://img.shields.io/maven-central/v/com.github.chrislusf/seaweedfs-client)](https://search.maven.org/search?q=g:com.github.chrislusf)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/seaweedfs)](https://artifacthub.io/packages/search?repo=seaweedfs)

![SeaweedFS Logo](https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/note/seaweedfs.png)

<h2 align="center"><a href="https://www.patreon.com/seaweedfs">Sponsor SeaweedFS via Patreon</a></h2>

SeaweedFS is an independent Apache-licensed open source project with its ongoing development made
possible entirely thanks to the support of these awesome [backers](https://github.com/seaweedfs/seaweedfs/blob/master/backers.md).
If you'd like to grow SeaweedFS even stronger, please consider joining our
<a href="https://www.patreon.com/seaweedfs">sponsors on Patreon</a>.

Your support will be really appreciated by me and other supporters!

<!--
<h4 align="center">Platinum</h4>

<p align="center">
  <a href="" target="_blank">
    Add your name or icon here
  </a>
</p>
-->

### Gold Sponsors
[![nodion](https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/note/sponsor_nodion.png)](https://www.nodion.com)
[![piknik](https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/note/piknik.png)](https://www.piknik.com)
[![keepsec](https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/note/keepsec.png)](https://www.keepsec.ca)

---

- [Download Binaries for different platforms](https://github.com/seaweedfs/seaweedfs/releases/latest)
- [SeaweedFS on Slack](https://join.slack.com/t/seaweedfs/shared_invite/enQtMzI4MTMwMjU2MzA3LTEyYzZmZWYzOGQ3MDJlZWMzYmI0OTE4OTJiZjJjODBmMzUxNmYwODg0YjY3MTNlMjBmZDQ1NzQ5NDJhZWI2ZmY)
- [SeaweedFS on Twitter](https://twitter.com/SeaweedFS)
- [SeaweedFS on Telegram](https://t.me/Seaweedfs) 
- [SeaweedFS on Reddit](https://www.reddit.com/r/SeaweedFS/)
- [SeaweedFS Mailing List](https://groups.google.com/d/forum/seaweedfs)
- [Wiki Documentation](https://github.com/seaweedfs/seaweedfs/wiki)
- [SeaweedFS White Paper](https://github.com/seaweedfs/seaweedfs/wiki/SeaweedFS_Architecture.pdf)
- [SeaweedFS Introduction Slides 2021.5](https://docs.google.com/presentation/d/1DcxKWlINc-HNCjhYeERkpGXXm6nTCES8mi2W5G0Z4Ts/edit?usp=sharing)
- [SeaweedFS Introduction Slides 2019.3](https://www.slideshare.net/chrislusf/seaweedfs-introduction)

Table of Contents
=================

* [Quick Start](#quick-start)
    * [Quick Start for S3 API on Docker](#quick-start-for-s3-api-on-docker)
    * [Quick Start with Single Binary](#quick-start-with-single-binary)
    * [Quick Start SeaweedFS S3 on AWS](#quick-start-seaweedfs-s3-on-aws)
* [Introduction](#introduction)
* [Features](#features)
    * [Additional Features](#additional-features)
    * [Filer Features](#filer-features)
* [Example: Using Seaweed Object Store](#example-using-seaweed-object-store)
* [Architecture](#object-store-architecture)
* [Compared to Other File Systems](#compared-to-other-file-systems)
    * [Compared to HDFS](#compared-to-hdfs)
    * [Compared to GlusterFS, Ceph](#compared-to-glusterfs-ceph)
    * [Compared to GlusterFS](#compared-to-glusterfs)
    * [Compared to Ceph](#compared-to-ceph)
    * [Compared to Minio](#compared-to-minio)
* [Dev Plan](#dev-plan)
* [Installation Guide](#installation-guide)
* [Disk Related Topics](#disk-related-topics)
* [Benchmark](#benchmark)
* [License](#license)

# Quick Start #

## Quick Start for S3 API on Docker ##

`docker run -p 8333:8333 chrislusf/seaweedfs server -s3`

## Quick Start with Single Binary ##
* Download the latest binary from https://github.com/seaweedfs/seaweedfs/releases and unzip a single binary file `weed` or `weed.exe`. Or run `go install github.com/seaweedfs/seaweedfs/weed@latest`.
* Run `weed server -dir=/some/data/dir -s3` to start one master, one volume server, one filer, and one S3 gateway.

Also, to increase capacity, just add more volume servers by running `weed volume -dir="/some/data/dir2" -mserver="<master_host>:9333" -port=8081` locally, or on a different machine, or on thousands of machines. That is it!

## Quick Start SeaweedFS S3 on AWS ##
* Setup fast production-ready [SeaweedFS S3 on AWS with cloudformation](https://aws.amazon.com/marketplace/pp/prodview-nzelz5gprlrjc)

# Introduction #

SeaweedFS is a simple and highly scalable distributed file system. There are two objectives:

1. to store billions of files!
2. to serve the files fast!

SeaweedFS started as an Object Store to handle small files efficiently. 
Instead of managing all file metadata in a central master, 
the central master only manages volumes on volume servers, 
and these volume servers manage files and their metadata. 
This relieves concurrency pressure from the central master and spreads file metadata into volume servers, 
allowing faster file access (O(1), usually just one disk read operation).

There is only 40 bytes of disk storage overhead for each file's metadata. 
It is so simple with O(1) disk reads that you are welcome to challenge the performance with your actual use cases.

SeaweedFS started by implementing [Facebook's Haystack design paper](http://www.usenix.org/event/osdi10/tech/full_papers/Beaver.pdf). 
Also, SeaweedFS implements erasure coding with ideas from 
[f4: Facebook’s Warm BLOB Storage System](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-muralidhar.pdf), and has a lot of similarities with [Facebook’s Tectonic Filesystem](https://www.usenix.org/system/files/fast21-pan.pdf)

On top of the object store, optional [Filer] can support directories and POSIX attributes. 
Filer is a separate linearly-scalable stateless server with customizable metadata stores, 
e.g., MySql, Postgres, Redis, Cassandra, HBase, Mongodb, Elastic Search, LevelDB, RocksDB, Sqlite, MemSql, TiDB, Etcd, CockroachDB, YDB, etc.

For any distributed key value stores, the large values can be offloaded to SeaweedFS. 
With the fast access speed and linearly scalable capacity, 
SeaweedFS can work as a distributed [Key-Large-Value store][KeyLargeValueStore].

SeaweedFS can transparently integrate with the cloud. 
With hot data on local cluster, and warm data on the cloud with O(1) access time, 
SeaweedFS can achieve both fast local access time and elastic cloud storage capacity.
What's more, the cloud storage access API cost is minimized. 
Faster and cheaper than direct cloud storage!

[Back to TOC](#table-of-contents)

# Features #
## Additional Features ##
* Can choose no replication or different replication levels, rack and data center aware.
* Automatic master servers failover - no single point of failure (SPOF).
* Automatic Gzip compression depending on file MIME type.
* Automatic compaction to reclaim disk space after deletion or update.
* [Automatic entry TTL expiration][VolumeServerTTL].
* Any server with some disk space can add to the total storage space.
* Adding/Removing servers does **not** cause any data re-balancing unless triggered by admin commands.
* Optional picture resizing.
* Support ETag, Accept-Range, Last-Modified, etc.
* Support in-memory/leveldb/readonly mode tuning for memory/performance balance.
* Support rebalancing the writable and readonly volumes.
* [Customizable Multiple Storage Tiers][TieredStorage]: Customizable storage disk types to balance performance and cost.
* [Transparent cloud integration][CloudTier]: unlimited capacity via tiered cloud storage for warm data.
* [Erasure Coding for warm storage][ErasureCoding]  Rack-Aware 10.4 erasure coding reduces storage cost and increases availability.

[Back to TOC](#table-of-contents)

## Filer Features ##
* [Filer server][Filer] provides "normal" directories and files via HTTP.
* [File TTL][FilerTTL] automatically expires file metadata and actual file data.
* [Mount filer][Mount] reads and writes files directly as a local directory via FUSE.
* [Filer Store Replication][FilerStoreReplication] enables HA for filer meta data stores.
* [Active-Active Replication][ActiveActiveAsyncReplication] enables asynchronous one-way or two-way cross cluster continuous replication.
* [Amazon S3 compatible API][AmazonS3API] accesses files with S3 tooling.
* [Hadoop Compatible File System][Hadoop] accesses files from Hadoop/Spark/Flink/etc or even runs HBase.
* [Async Replication To Cloud][BackupToCloud] has extremely fast local access and backups to Amazon S3, Google Cloud Storage, Azure, BackBlaze.
* [WebDAV] accesses as a mapped drive on Mac and Windows, or from mobile devices.
* [AES256-GCM Encrypted Storage][FilerDataEncryption] safely stores the encrypted data.
* [Super Large Files][SuperLargeFiles] stores large or super large files in tens of TB.
* [Cloud Drive][CloudDrive] mounts cloud storage to local cluster, cached for fast read and write with asynchronous write back.
* [Gateway to Remote Object Store][GatewayToRemoteObjectStore] mirrors bucket operations to remote object storage, in addition to [Cloud Drive][CloudDrive]

## Kubernetes ##
* [Kubernetes CSI Driver][SeaweedFsCsiDriver] A Container Storage Interface (CSI) Driver. [![Docker Pulls](https://img.shields.io/docker/pulls/chrislusf/seaweedfs-csi-driver.svg?maxAge=4800)](https://hub.docker.com/r/chrislusf/seaweedfs-csi-driver/)
* [SeaweedFS Operator](https://github.com/seaweedfs/seaweedfs-operator)

[Filer]: https://github.com/seaweedfs/seaweedfs/wiki/Directories-and-Files
[SuperLargeFiles]: https://github.com/seaweedfs/seaweedfs/wiki/Data-Structure-for-Large-Files
[Mount]: https://github.com/seaweedfs/seaweedfs/wiki/FUSE-Mount
[AmazonS3API]: https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API
[BackupToCloud]: https://github.com/seaweedfs/seaweedfs/wiki/Async-Replication-to-Cloud
[Hadoop]: https://github.com/seaweedfs/seaweedfs/wiki/Hadoop-Compatible-File-System
[WebDAV]: https://github.com/seaweedfs/seaweedfs/wiki/WebDAV
[ErasureCoding]: https://github.com/seaweedfs/seaweedfs/wiki/Erasure-coding-for-warm-storage
[TieredStorage]: https://github.com/seaweedfs/seaweedfs/wiki/Tiered-Storage
[CloudTier]: https://github.com/seaweedfs/seaweedfs/wiki/Cloud-Tier
[FilerDataEncryption]: https://github.com/seaweedfs/seaweedfs/wiki/Filer-Data-Encryption
[FilerTTL]: https://github.com/seaweedfs/seaweedfs/wiki/Filer-Stores
[VolumeServerTTL]: https://github.com/seaweedfs/seaweedfs/wiki/Store-file-with-a-Time-To-Live
[SeaweedFsCsiDriver]: https://github.com/seaweedfs/seaweedfs-csi-driver
[ActiveActiveAsyncReplication]: https://github.com/seaweedfs/seaweedfs/wiki/Filer-Active-Active-cross-cluster-continuous-synchronization
[FilerStoreReplication]: https://github.com/seaweedfs/seaweedfs/wiki/Filer-Store-Replication
[KeyLargeValueStore]: https://github.com/seaweedfs/seaweedfs/wiki/Filer-as-a-Key-Large-Value-Store
[CloudDrive]: https://github.com/seaweedfs/seaweedfs/wiki/Cloud-Drive-Architecture
[GatewayToRemoteObjectStore]: https://github.com/seaweedfs/seaweedfs/wiki/Gateway-to-Remote-Object-Storage


[Back to TOC](#table-of-contents)

## Example: Using Seaweed Object Store ##

By default, the master node runs on port 9333, and the volume nodes run on port 8080.
Let's start one master node, and two volume nodes on port 8080 and 8081. Ideally, they should be started from different machines. We'll use localhost as an example.

SeaweedFS uses HTTP REST operations to read, write, and delete. The responses are in JSON or JSONP format.

### Start Master Server ###

```
> ./weed master
```

### Start Volume Servers ###

```
> weed volume -dir="/tmp/data1" -max=5  -mserver="localhost:9333" -port=8080 &
> weed volume -dir="/tmp/data2" -max=10 -mserver="localhost:9333" -port=8081 &
```

### Write File ###

To upload a file: first, send a HTTP POST, PUT, or GET request to `/dir/assign` to get an `fid` and a volume server URL:

```
> curl http://localhost:9333/dir/assign
{"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
```

Second, to store the file content, send a HTTP multi-part POST request to `url + '/' + fid` from the response:

```
> curl -F file=@/home/chris/myphoto.jpg http://127.0.0.1:8080/3,01637037d6
{"name":"myphoto.jpg","size":43234,"eTag":"1cc0118e"}
```

To update, send another POST request with updated file content.

For deletion, send an HTTP DELETE request to the same `url + '/' + fid` URL:

```
> curl -X DELETE http://127.0.0.1:8080/3,01637037d6
```

### Save File Id ###

Now, you can save the `fid`, 3,01637037d6 in this case, to a database field.

The number 3 at the start represents a volume id. After the comma, it's one file key, 01, and a file cookie, 637037d6.

The volume id is an unsigned 32-bit integer. The file key is an unsigned 64-bit integer. The file cookie is an unsigned 32-bit integer, used to prevent URL guessing.

The file key and file cookie are both coded in hex. You can store the <volume id, file key, file cookie> tuple in your own format, or simply store the `fid` as a string.

If stored as a string, in theory, you would need 8+1+16+8=33 bytes. A char(33) would be enough, if not more than enough, since most uses will not need 2^32 volumes.

If space is really a concern, you can store the file id in your own format. You would need one 4-byte integer for volume id, 8-byte long number for file key, and a 4-byte integer for the file cookie. So 16 bytes are more than enough.

### Read File ###

Here is an example of how to render the URL.

First look up the volume server's URLs by the file's volumeId:

```
> curl http://localhost:9333/dir/lookup?volumeId=3
{"volumeId":"3","locations":[{"publicUrl":"localhost:8080","url":"localhost:8080"}]}
```

Since (usually) there are not too many volume servers, and volumes don't move often, you can cache the results most of the time. Depending on the replication type, one volume can have multiple replica locations. Just randomly pick one location to read.

Now you can take the public URL, render the URL or directly read from the volume server via URL:

```
 http://localhost:8080/3,01637037d6.jpg
```

Notice we add a file extension ".jpg" here. It's optional and just one way for the client to specify the file content type.

If you want a nicer URL, you can use one of these alternative URL formats:

```
 http://localhost:8080/3/01637037d6/my_preferred_name.jpg
 http://localhost:8080/3/01637037d6.jpg
 http://localhost:8080/3,01637037d6.jpg
 http://localhost:8080/3/01637037d6
 http://localhost:8080/3,01637037d6
```

If you want to get a scaled version of an image, you can add some params:

```
http://localhost:8080/3/01637037d6.jpg?height=200&width=200
http://localhost:8080/3/01637037d6.jpg?height=200&width=200&mode=fit
http://localhost:8080/3/01637037d6.jpg?height=200&width=200&mode=fill
```

### Rack-Aware and Data Center-Aware Replication ###

SeaweedFS applies the replication strategy at a volume level. So, when you are getting a file id, you can specify the replication strategy. For example:

```
curl http://localhost:9333/dir/assign?replication=001
```

The replication parameter options are:

```
000: no replication
001: replicate once on the same rack
010: replicate once on a different rack, but same data center
100: replicate once on a different data center
200: replicate twice on two different data center
110: replicate once on a different rack, and once on a different data center
```

More details about replication can be found [on the wiki][Replication].

[Replication]: https://github.com/seaweedfs/seaweedfs/wiki/Replication

You can also set the default replication strategy when starting the master server.

### Allocate File Key on Specific Data Center ###

Volume servers can be started with a specific data center name:

```
 weed volume -dir=/tmp/1 -port=8080 -dataCenter=dc1
 weed volume -dir=/tmp/2 -port=8081 -dataCenter=dc2
```

When requesting a file key, an optional "dataCenter" parameter can limit the assigned volume to the specific data center. For example, this specifies that the assigned volume should be limited to 'dc1':

```
 http://localhost:9333/dir/assign?dataCenter=dc1
```

### Other Features ###
  * [No Single Point of Failure][feat-1]
  * [Insert with your own keys][feat-2]
  * [Chunking large files][feat-3]
  * [Collection as a Simple Name Space][feat-4]

[feat-1]: https://github.com/seaweedfs/seaweedfs/wiki/Failover-Master-Server
[feat-2]: https://github.com/seaweedfs/seaweedfs/wiki/Optimization#insert-with-your-own-keys
[feat-3]: https://github.com/seaweedfs/seaweedfs/wiki/Optimization#upload-large-files
[feat-4]: https://github.com/seaweedfs/seaweedfs/wiki/Optimization#collection-as-a-simple-name-space

[Back to TOC](#table-of-contents)

## Object Store Architecture ##

Usually distributed file systems split each file into chunks, a central master keeps a mapping of filenames, chunk indices to chunk handles, and also which chunks each chunk server has.

The main drawback is that the central master can't handle many small files efficiently, and since all read requests need to go through the chunk master, so it might not scale well for many concurrent users.

Instead of managing chunks, SeaweedFS manages data volumes in the master server. Each data volume is 32GB in size, and can hold a lot of files. And each storage node can have many data volumes. So the master node only needs to store the metadata about the volumes, which is a fairly small amount of data and is generally stable.

The actual file metadata is stored in each volume on volume servers. Since each volume server only manages metadata of files on its own disk, with only 16 bytes for each file, all file access can read file metadata just from memory and only needs one disk operation to actually read file data.

For comparison, consider that an xfs inode structure in Linux is 536 bytes.

### Master Server and Volume Server ###

The architecture is fairly simple. The actual data is stored in volumes on storage nodes. One volume server can have multiple volumes, and can both support read and write access with basic authentication.

All volumes are managed by a master server. The master server contains the volume id to volume server mapping. This is fairly static information, and can be easily cached.

On each write request, the master server also generates a file key, which is a growing 64-bit unsigned integer. Since write requests are not generally as frequent as read requests, one master server should be able to handle the concurrency well.

### Write and Read files ###

When a client sends a write request, the master server returns (volume id, file key, file cookie, volume node URL) for the file. The client then contacts the volume node and POSTs the file content.

When a client needs to read a file based on (volume id, file key, file cookie), it asks the master server by the volume id for the (volume node URL, volume node public URL), or retrieves this from a cache. Then the client can GET the content, or just render the URL on web pages and let browsers fetch the content.

Please see the example for details on the write-read process.

### Storage Size ###

In the current implementation, each volume can hold 32 gibibytes (32GiB or 8x2^32 bytes). This is because we align content to 8 bytes. We can easily increase this to 64GiB, or 128GiB, or more, by changing 2 lines of code, at the cost of some wasted padding space due to alignment.

There can be 4 gibibytes (4GiB or 2^32 bytes) of volumes. So the total system size is 8 x 4GiB x 4GiB which is 128 exbibytes (128EiB or 2^67 bytes).

Each individual file size is limited to the volume size.

### Saving memory ###

All file meta information stored on a volume server is readable from memory without disk access. Each file takes just a 16-byte map entry of <64bit key, 32bit offset, 32bit size>. Of course, each map entry has its own space cost for the map. But usually the disk space runs out before the memory does.

### Tiered Storage to the cloud ###

The local volume servers are much faster, while cloud storages have elastic capacity and are actually more cost-efficient if not accessed often (usually free to upload, but relatively costly to access). With the append-only structure and O(1) access time, SeaweedFS can take advantage of both local and cloud storage by offloading the warm data to the cloud.

Usually hot data are fresh and warm data are old. SeaweedFS puts the newly created volumes on local servers, and optionally upload the older volumes on the cloud. If the older data are accessed less often, this literally gives you unlimited capacity with limited local servers, and still fast for new data. 

With the O(1) access time, the network latency cost is kept at minimum. 

If the hot/warm data is split as 20/80, with 20 servers, you can achieve storage capacity of 100 servers. That's a cost saving of 80%! Or you can repurpose the 80 servers to store new data also, and get 5X storage throughput.

[Back to TOC](#table-of-contents)

## Compared to Other File Systems ##

Most other distributed file systems seem more complicated than necessary.

SeaweedFS is meant to be fast and simple, in both setup and operation. If you do not understand how it works when you reach here, we've failed! Please raise an issue with any questions or update this file with clarifications.

SeaweedFS is constantly moving forward. Same with other systems. These comparisons can be outdated quickly. Please help to keep them updated.

[Back to TOC](#table-of-contents)

### Compared to HDFS ###

HDFS uses the chunk approach for each file, and is ideal for storing large files.

SeaweedFS is ideal for serving relatively smaller files quickly and concurrently.

SeaweedFS can also store extra large files by splitting them into manageable data chunks, and store the file ids of the data chunks into a meta chunk. This is managed by "weed upload/download" tool, and the weed master or volume servers are agnostic about it.

[Back to TOC](#table-of-contents)

### Compared to GlusterFS, Ceph ###

The architectures are mostly the same. SeaweedFS aims to store and read files fast, with a simple and flat architecture. The main differences are

* SeaweedFS optimizes for small files, ensuring O(1) disk seek operation, and can also handle large files.
* SeaweedFS statically assigns a volume id for a file. Locating file content becomes just a lookup of the volume id, which can be easily cached.
* SeaweedFS Filer metadata store can be any well-known and proven data store, e.g., Redis, Cassandra, HBase, Mongodb, Elastic Search, MySql, Postgres, Sqlite, MemSql, TiDB, CockroachDB, Etcd, YDB etc, and is easy to customize.
* SeaweedFS Volume server also communicates directly with clients via HTTP, supporting range queries, direct uploads, etc.

| System         | File Metadata                   | File Content Read| POSIX  | REST API | Optimized for large number of small files |
| -------------  | ------------------------------- | ---------------- | ------ | -------- | ------------------------- |
| SeaweedFS      | lookup volume id, cacheable     | O(1) disk seek   |        | Yes      | Yes                       |
| SeaweedFS Filer| Linearly Scalable, Customizable | O(1) disk seek   | FUSE   | Yes      | Yes                       |
| GlusterFS      | hashing          |                  | FUSE, NFS          |          |                           |
| Ceph           | hashing + rules  |                  | FUSE               | Yes      |                           |
| MooseFS        | in memory        |                  | FUSE               |       | No                          |
| MinIO          | separate meta file for each file  |                  |         | Yes   | No                          |

[Back to TOC](#table-of-contents)

### Compared to GlusterFS ###

GlusterFS stores files, both directories and content, in configurable volumes called "bricks".

GlusterFS hashes the path and filename into ids, and assigned to virtual volumes, and then mapped to "bricks".

[Back to TOC](#table-of-contents)

### Compared to MooseFS ###

MooseFS chooses to neglect small file issue. From moosefs 3.0 manual, "even a small file will occupy 64KiB plus additionally 4KiB of checksums and 1KiB for the header", because it "was initially designed for keeping large amounts (like several thousands) of very big files"

MooseFS Master Server keeps all meta data in memory. Same issue as HDFS namenode. 

[Back to TOC](#table-of-contents)

### Compared to Ceph ###

Ceph can be setup similar to SeaweedFS as a key->blob store. It is much more complicated, with the need to support layers on top of it. [Here is a more detailed comparison](https://github.com/seaweedfs/seaweedfs/issues/120)

SeaweedFS has a centralized master group to look up free volumes, while Ceph uses hashing and metadata servers to locate its objects. Having a centralized master makes it easy to code and manage.

Ceph, like SeaweedFS, is based on the object store RADOS. Ceph is rather complicated with mixed reviews.

Ceph uses CRUSH hashing to automatically manage data placement, which is efficient to locate the data. But the data has to be placed according to the CRUSH algorithm. Any wrong configuration would cause data loss. Topology changes, such as adding new servers to increase capacity, will cause data migration with high IO cost to fit the CRUSH algorithm. SeaweedFS places data by assigning them to any writable volumes. If writes to one volume failed, just pick another volume to write. Adding more volumes is also as simple as it can be.

SeaweedFS is optimized for small files. Small files are stored as one continuous block of content, with at most 8 unused bytes between files. Small file access is O(1) disk read.

SeaweedFS Filer uses off-the-shelf stores, such as MySql, Postgres, Sqlite, Mongodb, Redis, Elastic Search, Cassandra, HBase, MemSql, TiDB, CockroachCB, Etcd, YDB, to manage file directories. These stores are proven, scalable, and easier to manage.

| SeaweedFS         | comparable to Ceph | advantage |
| -------------  | ------------- | ---------------- |
| Master  | MDS | simpler |
| Volume  | OSD | optimized for small files |
| Filer  | Ceph FS | linearly scalable, Customizable, O(1) or O(logN) |

[Back to TOC](#table-of-contents)

### Compared to MinIO ###

MinIO follows AWS S3 closely and is ideal for testing for S3 API. It has good UI, policies, versionings, etc. SeaweedFS is trying to catch up here. It is also possible to put MinIO as a gateway in front of SeaweedFS later.

MinIO metadata are in simple files. Each file write will incur extra writes to corresponding meta file.

MinIO does not have optimization for lots of small files. The files are simply stored as is to local disks.
Plus the extra meta file and shards for erasure coding, it only amplifies the LOSF problem.

MinIO has multiple disk IO to read one file. SeaweedFS has O(1) disk reads, even for erasure coded files.

MinIO has full-time erasure coding. SeaweedFS uses replication on hot data for faster speed and optionally applies erasure coding on warm data.

MinIO does not have POSIX-like API support.

MinIO has specific requirements on storage layout. It is not flexible to adjust capacity. In SeaweedFS, just start one volume server pointing to the master. That's all.

## Dev Plan ##

* More tools and documentation, on how to manage and scale the system.
* Read and write stream data.
* Support structured data.

This is a super exciting project! And we need helpers and [support](https://www.patreon.com/seaweedfs)!

[Back to TOC](#table-of-contents)

## Installation Guide ##

> Installation guide for users who are not familiar with golang

Step 1: install go on your machine and setup the environment by following the instructions at:

https://golang.org/doc/install

make sure to define your $GOPATH


Step 2: checkout this repo:
```bash
git clone https://github.com/seaweedfs/seaweedfs.git
```
Step 3: download, compile, and install the project by executing the following command

```bash
cd seaweedfs/weed && make install
```

Once this is done, you will find the executable "weed" in your `$GOPATH/bin` directory

[Back to TOC](#table-of-contents)

## Disk Related Topics ##

### Hard Drive Performance ###

When testing read performance on SeaweedFS, it basically becomes a performance test of your hard drive's random read speed. Hard drives usually get 100MB/s~200MB/s.

### Solid State Disk ###

To modify or delete small files, SSD must delete a whole block at a time, and move content in existing blocks to a new block. SSD is fast when brand new, but will get fragmented over time and you have to garbage collect, compacting blocks. SeaweedFS is friendly to SSD since it is append-only. Deletion and compaction are done on volume level in the background, not slowing reading and not causing fragmentation.

[Back to TOC](#table-of-contents)

## Benchmark ##

My Own Unscientific Single Machine Results on Mac Book with Solid State Disk, CPU: 1 Intel Core i7 2.6GHz.

Write 1 million 1KB file:
```
Concurrency Level:      16
Time taken for tests:   66.753 seconds
Completed requests:      1048576
Failed requests:        0
Total transferred:      1106789009 bytes
Requests per second:    15708.23 [#/sec]
Transfer rate:          16191.69 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.3      1.0       84.3      0.9

Percentage of the requests served within a certain time (ms)
   50%      0.8 ms
   66%      1.0 ms
   75%      1.1 ms
   80%      1.2 ms
   90%      1.4 ms
   95%      1.7 ms
   98%      2.1 ms
   99%      2.6 ms
  100%     84.3 ms
```

Randomly read 1 million files:
```
Concurrency Level:      16
Time taken for tests:   22.301 seconds
Completed requests:      1048576
Failed requests:        0
Total transferred:      1106812873 bytes
Requests per second:    47019.38 [#/sec]
Transfer rate:          48467.57 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.0      0.3       54.1      0.2

Percentage of the requests served within a certain time (ms)
   50%      0.3 ms
   90%      0.4 ms
   98%      0.6 ms
   99%      0.7 ms
  100%     54.1 ms
```

### Run WARP and launch a mixed benchmark. ###

```
make benchmark
warp: Benchmark data written to "warp-mixed-2023-10-16[102354]-l70a.csv.zst"                                                                                                                                                                                               
Mixed operations.
Operation: DELETE, 10%, Concurrency: 20, Ran 4m59s.
 * Throughput: 6.19 obj/s

Operation: GET, 45%, Concurrency: 20, Ran 5m0s.
 * Throughput: 279.85 MiB/s, 27.99 obj/s

Operation: PUT, 15%, Concurrency: 20, Ran 5m0s.
 * Throughput: 89.86 MiB/s, 8.99 obj/s

Operation: STAT, 30%, Concurrency: 20, Ran 5m0s.
 * Throughput: 18.63 obj/s

Cluster Total: 369.74 MiB/s, 61.79 obj/s, 0 errors over 5m0s.
```

To see segmented request statistics, use the --analyze.v parameter.
```
warp analyze --analyze.v warp-mixed-2023-10-16[102354]-l70a.csv.zst
18642 operations loaded... Done!
Mixed operations.
----------------------------------------
Operation: DELETE - total: 1854, 10.0%, Concurrency: 20, Ran 5m0s, starting 2023-10-16 10:23:57.115 +0500 +05
 * Throughput: 6.19 obj/s

Requests considered: 1855:
 * Avg: 104ms, 50%: 30ms, 90%: 207ms, 99%: 1.355s, Fastest: 1ms, Slowest: 4.613s, StdDev: 320ms

----------------------------------------
Operation: GET - total: 8388, 45.3%, Size: 10485760 bytes. Concurrency: 20, Ran 5m0s, starting 2023-10-16 10:23:57.12 +0500 +05
 * Throughput: 279.77 MiB/s, 27.98 obj/s

Requests considered: 8389:
 * Avg: 221ms, 50%: 106ms, 90%: 492ms, 99%: 1.739s, Fastest: 8ms, Slowest: 8.633s, StdDev: 383ms
 * TTFB: Avg: 81ms, Best: 2ms, 25th: 24ms, Median: 39ms, 75th: 65ms, 90th: 171ms, 99th: 669ms, Worst: 4.783s StdDev: 163ms
 * First Access: Avg: 240ms, 50%: 105ms, 90%: 511ms, 99%: 2.08s, Fastest: 12ms, Slowest: 8.633s, StdDev: 480ms
 * First Access TTFB: Avg: 88ms, Best: 2ms, 25th: 24ms, Median: 38ms, 75th: 64ms, 90th: 179ms, 99th: 919ms, Worst: 4.783s StdDev: 199ms
 * Last Access: Avg: 219ms, 50%: 106ms, 90%: 463ms, 99%: 1.782s, Fastest: 9ms, Slowest: 8.633s, StdDev: 416ms
 * Last Access TTFB: Avg: 81ms, Best: 2ms, 25th: 24ms, Median: 39ms, 75th: 65ms, 90th: 161ms, 99th: 657ms, Worst: 4.783s StdDev: 176ms

----------------------------------------
Operation: PUT - total: 2688, 14.5%, Size: 10485760 bytes. Concurrency: 20, Ran 5m0s, starting 2023-10-16 10:23:57.115 +0500 +05
 * Throughput: 89.83 MiB/s, 8.98 obj/s

Requests considered: 2689:
 * Avg: 1.165s, 50%: 878ms, 90%: 2.015s, 99%: 5.74s, Fastest: 99ms, Slowest: 8.264s, StdDev: 968ms

----------------------------------------
Operation: STAT - total: 5586, 30.2%, Concurrency: 20, Ran 5m0s, starting 2023-10-16 10:23:57.113 +0500 +05
 * Throughput: 18.63 obj/s

Requests considered: 5587:
 * Avg: 15ms, 50%: 11ms, 90%: 34ms, 99%: 80ms, Fastest: 0s, Slowest: 245ms, StdDev: 17ms
 * First Access: Avg: 14ms, 50%: 10ms, 90%: 33ms, 99%: 69ms, Fastest: 0s, Slowest: 203ms, StdDev: 16ms
 * Last Access: Avg: 15ms, 50%: 11ms, 90%: 34ms, 99%: 74ms, Fastest: 0s, Slowest: 203ms, StdDev: 17ms

Cluster Total: 369.64 MiB/s, 61.77 obj/s, 0 errors over 5m0s.
Total Errors:0.
```

[Back to TOC](#table-of-contents)

## License ##

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The text of this page is available for modification and reuse under the terms of the Creative Commons Attribution-Sharealike 3.0 Unported License and the GNU Free Documentation License (unversioned, with no invariant sections, front-cover texts, or back-cover texts).

[Back to TOC](#table-of-contents)

## Stargazers over time

[![Stargazers over time](https://starchart.cc/chrislusf/seaweedfs.svg)](https://starchart.cc/chrislusf/seaweedfs)
