Seaweed File System
=======

[![Build Status](https://travis-ci.org/chrislusf/weed-fs.svg?branch=master)](https://travis-ci.org/chrislusf/weed-fs)
[![GoDoc](https://godoc.org/github.com/chrislusf/weed-fs/go?status.svg)](https://godoc.org/github.com/chrislusf/weed-fs/go)
[![RTD](https://readthedocs.org/projects/weed-fs/badge/?version=latest)](http://weed-fs.readthedocs.org/en/latest/)


## Introduction

Seaweed-FS is a simple and highly scalable distributed file system. There are two objectives:

1. to store billions of files!
2. to serve the files fast!

Instead of supporting full POSIX file system semantics, Seaweed-FS choose to implement only a key~file mapping. Similar to the word "NoSQL", you can call it as "NoFS".

Instead of managing all file metadata in a central master, Seaweed-FS choose to manages file volumes in the central master, and let volume servers manage files and the metadata. This relieves concurrency pressure from the central master and spreads file metadata into volume servers' memories, allowing faster file access with just one disk read operation!

Seaweed-FS models after [Facebook's Haystack design paper](http://www.usenix.org/event/osdi10/tech/full_papers/Beaver.pdf). 

Seaweed-FS costs only 40 bytes disk storage for each file's metadata. It is so simple with O(1) disk read that you are welcome to challenge the performance with your actual use cases. 


![](https://api.bintray.com/packages/chrislusf/Weed-FS/seaweed/images/download.png)
https://bintray.com/chrislusf/Weed-FS/seaweed Download latest compiled binaries for different platforms here.

## Additional Features
* Can choose no replication or different replication level, rack and data center aware
* Automatic master servers failover. No single point of failure, SPOF.
* Automatic Gzip compression depending on file mime type
* Automatic compaction to reclaimed disk spaces after deletion or update
* Servers in the same cluster can have different disk spaces, different file systems, different OS
* Adding/Removing servers do not cause any data re-balancing
* Optional  [filer server](https://code.google.com/p/weed-fs/wiki/DirectoriesAndFiles) provides "normal" directories and files via http
* For jpeg pictures, optionally fix the orientation.
* Support Etag, Accept-Range, Last-Modified, etc.

## Example Usage
By default, the master node runs on port 9333, and the volume nodes runs on port 8080. 
Here I will start one master node, and two volume nodes on port 8080 and 8081. Ideally, they should be started from different machines. Here I just use localhost as example.

Seaweed-FS uses HTTP REST operations to write, read, delete. The return results are JSON or JSONP format.

### Start Master Server

```
> ./weed master

```

### Start Volume Servers ###

```
> weed volume -dir="/tmp/data1" -max=5  -mserver="localhost:9333" -port=8080 &
> weed volume -dir="/tmp/data2" -max=10 -mserver="localhost:9333" -port=8081 & 

```

### Write File ###
Here is a simple usage on how to save a file:

```
> curl http://localhost:9333/dir/assign
{"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
```
First, send a HTTP request to get an fid and a volume server url.

```
> curl -F file=@/home/chris/myphoto.jpg http://127.0.0.1:8080/3,01637037d6
{"size": 43234}
```
Second, send a HTTP multipart POST request to the volume server url+'/'+fid, to really store the file content.

For update, send another POST request with updated file content. 

For deletion, send a http DELETE request
```
> curl -X DELETE http://127.0.0.1:8080/3,01637037d6
```
### Save File Id ###
Now you can save the fid, 3,01637037d6 in this case, to some database field.

The number 3 here, is a volume id. After the comma, it's one file key, 01, and a file cookie, 637037d6.

The volume id is an unsigned 32 bit integer. The file key is an unsigned 64bit integer. The file cookie is an unsigned 32bit integer, used to prevent URL guessing.

The file key and file cookie are both coded in hex. You can store the <volume id, file key, file cookie> tuple in your own format, or simply store the fid as string. 

If stored as a string, in theory, you would need 8+1+16+8=33 bytes. A char(33) would be enough, if not more than enough, since most usage would not need 2^32 volumes.

If space is really a concern, you can store the file id in your own format. You would need one 4-byte integer for volume id, 8-byte long number for file key, 4-byte integer for file cookie. So 16 bytes are enough (more than enough).

### Read File ###
Here is the example on how to render the URL.
```
> curl http://localhost:9333/dir/lookup?volumeId=3
{"locations":[{"publicUrl":"localhost:8080","url":"localhost:8080"}]}
```
First lookup the volume server's URLs by the file's volumeId. However, since usually there are not too many volume servers, and volumes does not move often, you can cache the results most of the time. Depends on the replication type, one volume can have multiple replica locations. Just randomly pick one location to read.

Now you can take the public url, render the url or directly read from the volume server via url:
```
 http://localhost:8080/3,01637037d6.jpg
```
Notice we add an file extension ".jpg" here. It's optional and just one way for the client to specify the file content type.

If you want a nicer URL, you can use one of these alternative URL formats:
```
 http://localhost:8080/3/01637037d6/my_preferred_name.jpg
 http://localhost:8080/3/01637037d6.jpg
 http://localhost:8080/3,01637037d6.jpg
 http://localhost:8080/3/01637037d6
 http://localhost:8080/3,01637037d6
```

### Rack-Aware and Data Center-Aware Replication ###
Seaweed-FS apply the replication strategy on a volume level. So when you are getting a file id, you can specify the replication strategy. For example:
```
curl http://localhost:9333/dir/assign?replication=001
```

Here is the meaning of the replication parameter 
```
000: no replication
001: replicate once on the same rack
010: replicate once on a different rack, but same data center
100: replicate once on a different data center
200: replicate twice on two different data center
110: replicate once on a different rack, and once on a different data center
```

More details about replication can be found here:
https://code.google.com/p/weed-fs/wiki/RackDataCenterAwareReplication

You can also set the default replication strategy when starting the master server.

### Allocate File Key on specific data center ###
Volume servers can start with a specific data center name.
```
 weed volume -dir=/tmp/1 -port=8080 -dataCenter=dc1
 weed volume -dir=/tmp/2 -port=8081 -dataCenter=dc2
```
Or the master server can determine the data center via volume server's IP address and settings in weed.conf file.

Now when requesting a file key, an optional "dataCenter" parameter can limit the assigned volume to the specific data center. For example, this specify
```
 http://localhost:9333/dir/assign?dataCenter=dc1
```

### Other Features ###
  * [No Single Point of Failure](https://code.google.com/p/weed-fs/wiki/FailoverMasterServer)
  * [Insert  with your own keys](https://code.google.com/p/weed-fs/wiki/Optimization#Insert_with_your_own_keys)
  * [ Chunking large files](https://code.google.com/p/weed-fs/wiki/Optimization#Upload_large_files)
  * [Collection as a Simple Name Space](https://code.google.com/p/weed-fs/wiki/Optimization#Collection_as_a_Simple_Name_Space)

## Architecture ##
Usually distributed file system split each file into chunks, and a central master keeps a mapping of a filename and a chunk index to chunk handles, and also which chunks each chunk server has.

This has the draw back that the central master can not handle many small files efficiently, and since all read requests need to go through the chunk master, responses would be slow for many concurrent web users.

Instead of managing chunks, Seaweed-FS choose to manage data volumes in the master server. Each data volume is size 32GB, and can hold a lot of files. And each storage node can has many data volumes. So the master node only needs to store the metadata about the volumes, which is fairly small amount of data and pretty static most of the time.

The actual file metadata is stored in each volume on volume servers. Since each volume server only manage metadata of files on its own disk, and only 16 bytes for each file, all file access can read file metadata just from memory and only needs one disk operation to actually read file data.

For comparison, consider that an xfs inode structure in Linux is 536 bytes.

### Master Server and Volume Server ###
The architecture is fairly simple. The actual data is stored in volumes on storage nodes. One volume server can have multiple volumes, and can both support read and write access with basic authentication.

All volumes are managed by a master server. The master server contains volume id to volume server mapping. This is fairly static information, and could be cached easily.

On each write request, the master server also generates a file key, which is a growing 64bit unsigned integer. Since the write requests are not as busy as read requests, one master server should be able to handle the concurrency well.


### Write and Read files ###

When a client sends a write request, the master server returns <volume id, file key, file cookie, volume node url> for the file. The client then contact the volume node and POST the file content via REST.

When a client needs to read a file based on <volume id, file key, file cookie>, it can ask the master server by the <volum id> for the <volume node url, volume node public url>, or from cache. Then the client can HTTP GET the content via REST, or just render the URL on web pages and let browsers to fetch the content.

Please see the example for details on write-read process.

### Storage Size ###
In current implementation, each volume can be size of 8x2^32^=32G bytes. This is because of aligning contents to 8 bytes. We can be easily increased to 64G, or 128G, or more, by changing 2 lines of code, at the cost of some wasted padding space due to alignment.

There can be 2^32^ volumes. So total system size is 8 x 2^32^ x 2^32^ = 8 x 4G x 4G = 128GG bytes. (Sorry, I don't know the word for giga of giga bytes.)

Each individual file size is limited to the volume size.

### Saving memory ###
All file meta information on volume server is readable from memory without disk access. Each file just takes an 16-byte map entry of <64bit key, 32bit offset, 32bit size>. Of course, each map entry has its own the space cost for the map. But usually the disk runs out before the memory does.


## Compared to Other File Systems##
Frankly, I don't use other distributed file systems too often. All seems more complicated than necessary. Please correct me if anything here is wrong.

### Compared to Ceph ###
Ceph can be setup similar to Seaweed-FS as a key~blob store. It is much more complicated, with the need to support layers on top of it. Here is a more detailed comparison. https://code.google.com/p/weed-fs/issues/detail?id=44

Seaweed-FS is meant to be fast and simple, both during usage and during setup. If you do not understand how it works when you reach here, we failed! Jokes aside, you should not need any consulting service for it.

Seaweed-FS has a centralized master to lookup free volumes, while Ceph uses hashing to locate its objects. Having a centralized master makes it easy to code and manage. HDFS/GFS has the single name node for years. Seaweed-FS now support multiple master nodes.

Ceph hashing avoids SPOF, but makes it complicated when moving or adding servers.

### Compared to HDFS ###
HDFS uses the chunk approach for each file, and is ideal for streaming large files.

Seaweed-FS is ideal for serving relatively smaller files quickly and concurrently.

Seaweed-FS can also store extra large files by splitting them into manageable data chunks, and store the file ids of the data chunks into a meta chunk. This is managed by "weed upload/download" tool, and the weed master or volume servers are agnostic about it.

### Compared to MogileFS###
Seaweed-FS has 2 components: directory server, storage nodes.

MogileFS has 3 components: tracers, database, storage nodes.

One more layer means slower access, more operation complexity, more failure possibility.

### Compared to GlusterFS ###
Seaweed-FS is not POSIX compliant, and has simple implementation.

GlusterFS is POSIX compliant, much more complex.

### Compared to Mongo's GridFS ###
Mongo's GridFS splits files into chunks and manage chunks in the central mongodb. For every read or write request, the database needs to query the metadata. It's OK if this is not a bottleneck yet, but for a lot of concurrent reads this unnecessary query could slow things down.

Since files are chunked(default to 256KB), there will be multiple metadata readings and multiple chunk readings, linear to the file size. One  2.56MB file would require at least 20 disk read requests.

On the contrary, Seaweed-FS uses large file volume of 32G size to store lots of files, and only manages file volumes in the master server. Each volume manages file metadata themselves. So all the file metadata is spread onto the volume nodes memories, and just one disk read is needed.

## Dev plan ##

More tools and documentation, on how to maintain and scale the system. For example, how to move volumes, automatically balancing data, how to grow volumes, how to check system status, etc.

This is a super exciting project! And I need helpers!


## Installation guide for users who are not familiar with golang

step 1: install go on your machine and setup the environment by following the instructions from the following link:

https://golang.org/doc/install

make sure you set up your $GOPATH


step 2: also you may need to install Mercurial by following the instructions below

http://mercurial.selenic.com/downloads


step 3: download, compile, and install the project by executing the following command 

go get github.com/chrislusf/weed-fs/go/weed

once this is done, you should see the executable "weed" under $GOPATH/bin

step 4: after you modify your code locally, you could start a local build by calling "go install" under $GOPATH/src/github.com/chrislusf/weed-fs/go/weed

## Reference

For pre-compiled releases,
 https://bintray.com/chrislusf/Weed-FS/seaweed

## Disk Related topics ##

### Hard Drive Performance ###
When testing read performance on Seaweed-FS, it basically becomes performance test your hard drive's random read speed. Hard Drive usually get 100MB/s~200MB/s.

### Solid State Disk

To modify or delete small files, SSD must delete a whole block at a time, and move content in existing blocks to a new block. SSD is fast when brand new, but will get fragmented over time and you have to garbage collect, compacting blocks. Seaweed-FS is friendly to SSD since it is append-only. Deletion and compaction are done on volume level in the background, not slowing reading and not causing fragmentation.

## Not Planned

POSIX support


## Benchmark

My Own Unscientific Single Machine Results on Mac Book with Solid State Disk, CPU: 1 Intel Core i7 2.2GHz.

Write 1 million 1KB file:
```
Concurrency Level:      64
Time taken for tests:   182.456 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1073741824 bytes
Requests per second:    5747.01 [#/sec]
Transfer rate:          5747.01 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.3      10.9       430.9      5.7

Percentage of the requests served within a certain time (ms)
   50%     10.2 ms
   66%     12.0 ms
   75%     12.6 ms
   80%     12.9 ms
   90%     14.0 ms
   95%     14.9 ms
   98%     16.2 ms
   99%     17.3 ms
  100%    430.9 ms
```

Randomly read 1 million files:
```
Concurrency Level:      64
Time taken for tests:   80.732 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1073741824 bytes
Requests per second:    12988.37 [#/sec]
Transfer rate:          12988.37 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.0      4.7       254.3      6.3

Percentage of the requests served within a certain time (ms)
   50%      2.6 ms
   66%      2.9 ms
   75%      3.7 ms
   80%      4.7 ms
   90%     10.3 ms
   95%     16.6 ms
   98%     26.3 ms
   99%     34.8 ms
  100%    254.3 ms
```
