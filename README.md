# SeaweedFS

[![Build Status](https://travis-ci.org/chrislusf/seaweedfs.svg?branch=master)](https://travis-ci.org/chrislusf/seaweedfs)
[![GoDoc](https://godoc.org/github.com/chrislusf/seaweedfs/weed?status.svg)](https://godoc.org/github.com/chrislusf/seaweedfs/weed)
[![Wiki](https://img.shields.io/badge/docs-wiki-blue.svg)](https://github.com/chrislusf/seaweedfs/wiki)

![SeaweedFS Logo](https://raw.githubusercontent.com/chrislusf/seaweedfs/master/note/seaweedfs.png)

<h2 align="center">Supporting SeaweedFS</h2>

SeaweedFS is Apache-licensed open source project, independent project with its ongoing development made 
possible entirely thanks to the support by these awesome [backers](https://github.com/chrislusf/seaweedfs/blob/master/backers.md). 
If you'd like to grow SeaweedFS even stronger, please consider to 
<a href="https://www.patreon.com/seaweedfs">Sponsor SeaweedFS via Patreon</a>.

Platinum ($2500/month), Gold ($500/month): put your company logo on the SeaweedFS github page
Generous Backer($50/month), Backer($10/month): put your name on the SeaweedFS backer page.

Your support will be really appreciated by me and other supporters!

<h3 align="center"><a href="https://www.patreon.com/seaweedfs">Sponsors SeaweedFS via Patreon</a></h3>

<h4 align="center">Platinum</h4>

<p align="center">
  <a href="" target="_blank">
    Add your name or icon here
  </a>
</p>

<h4 align="center">Gold</h4>

<table>
  <tbody>
    <tr>
      <td align="center" valign="middle">
        <a href="" target="_blank">
          Add your name or icon here
        </a>
      </td>
    </tr>
    <tr></tr>
  </tbody>
</table>

---


- [Download Binaries for different platforms](https://github.com/chrislusf/seaweedfs/releases/latest)
- [SeaweedFS Mailing List](https://groups.google.com/d/forum/seaweedfs)
- [Wiki Documentation](https://github.com/chrislusf/seaweedfs/wiki)


## Introduction

SeaweedFS is a simple and highly scalable distributed file system. There are two objectives:

1. to store billions of files!
2. to serve the files fast!

Instead of supporting full POSIX file system semantics, SeaweedFS chooses to implement only a key->file mapping. Similar to "NoSQL", you might call it "NoFS".

Instead of managing all file metadata in a central master, the central master only manages file volumes, and it lets these volume servers manage files and their metadata. This relieves concurrency pressure from the central master and spreads file metadata into volume servers, allowing faster file access (just one disk read operation).

There is only a 40 bytes disk storage overhead for each file's metadata. It is so simple with O(1) disk read that you are welcome to challenge the performance with your actual use cases.

SeaweedFS started by implementing [Facebook's Haystack design paper](http://www.usenix.org/event/osdi10/tech/full_papers/Beaver.pdf). SeaweedFS is currently growing, with more features on the way.

## Additional Features
* Can choose no replication or different replication level, rack and data center aware
* Automatic master servers failover - no single point of failure (SPOF)
* Automatic Gzip compression depending on file mime type
* Automatic compaction to reclaimed disk spaces after deletion or update
* Servers in the same cluster can have different disk spaces, file systems, OS etc.
* Adding/Removing servers does **not** cause any data re-balancing
* Optional [filer server][Filer] provides "normal" directories and files via http
* Optionally fix the orientation for jpeg pictures
* Support Etag, Accept-Range, Last-Modified, etc.
* Support in-memory/leveldb/boltdb/btree mode tuning for memory/performance balance.

[Filer]: https://github.com/chrislusf/seaweedfs/wiki/Filer

## Example Usage
By default, the master node runs on port 9333, and the volume nodes run on port 8080.
Here I will start one master node, and two volume nodes on port 8080 and 8081. Ideally, they should be started from different machines. I just use localhost as an example.

SeaweedFS uses HTTP REST operations to write, read, delete. The responses are in JSON or JSONP format.

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

To upload a file: first, send a HTTP POST, PUT, or GET request to `/dir/assign` to get an `fid` and a volume server url:

```
> curl -X POST http://localhost:9333/dir/assign
{"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
```

Second, to store the file content, send a HTTP multi-part PUT or POST request to `url + '/' + fid` from the response:

```
> curl -X PUT -F file=@/home/chris/myphoto.jpg http://127.0.0.1:8080/3,01637037d6
{"size": 43234}
```

To update, send another PUT or POST request with updated file content.

For deletion, send an HTTP DELETE request to the same `url + '/' + fid` URL:

```
> curl -X DELETE http://127.0.0.1:8080/3,01637037d6
```
### Save File Id ###

Now you can save the `fid`, 3,01637037d6 in this case, to a database field.

The number 3 at the start represents a volume id. After the comma, it's one file key, 01, and a file cookie, 637037d6.

The volume id is an unsigned 32-bit integer. The file key is an unsigned 64-bit integer. The file cookie is an unsigned 32-bit integer, used to prevent URL guessing.

The file key and file cookie are both coded in hex. You can store the <volume id, file key, file cookie> tuple in your own format, or simply store the `fid` as string.

If stored as a string, in theory, you would need 8+1+16+8=33 bytes. A char(33) would be enough, if not more than enough, since most usage would not need 2^32 volumes.

If space is really a concern, you can store the file id in your own format. You would need one 4-byte integer for volume id, 8-byte long number for file key, 4-byte integer for file cookie. So 16 bytes are more than enough.

### Read File ###

Here is an example of how to render the URL.

First look up the volume server's URLs by the file's volumeId:

```
> curl http://localhost:9333/dir/lookup?volumeId=3
{"locations":[{"publicUrl":"localhost:8080","url":"localhost:8080"}]}
```

Since (usually) there are not too many volume servers, and volumes don't move often, you can cache the results most of the time. Depending on the replication type, one volume can have multiple replica locations. Just randomly pick one location to read.

Now you can take the public url, render the url or directly read from the volume server via url:

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
curl -X POST http://localhost:9333/dir/assign?replication=001
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

[Replication]: https://github.com/chrislusf/seaweedfs/wiki/Replication

You can also set the default replication strategy when starting the master server.

### Allocate File Key on specific data center ###

Volume servers can start with a specific data center name:

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

[feat-1]: https://github.com/chrislusf/seaweedfs/wiki/Failover-Master-Server
[feat-2]: https://github.com/chrislusf/seaweedfs/wiki/Optimization#insert-with-your-own-keys
[feat-3]: https://github.com/chrislusf/seaweedfs/wiki/Optimization#upload-large-files
[feat-4]: https://github.com/chrislusf/seaweedfs/wiki/Optimization#collection-as-a-simple-name-space

## Architecture ##

Usually distributed file systems split each file into chunks, a central master keeps a mapping of filenames, chunk indices to chunk handles, and also which chunks each chunk server has.

The main drawback is that the central master can't handle many small files efficiently, and since all read requests need to go through the chunk master, might not scale well for many concurrent users.

Instead of managing chunks, SeaweedFS manages data volumes in the master server. Each data volume is size 32GB, and can hold a lot of files. And each storage node can have many data volumes. So the master node only needs to store the metadata about the volumes, which is fairly small amount of data and is generally stable.

The actual file metadata is stored in each volume on volume servers. Since each volume server only manages metadata of files on its own disk, with only 16 bytes for each file, all file access can read file metadata just from memory and only needs one disk operation to actually read file data.

For comparison, consider that an xfs inode structure in Linux is 536 bytes.

### Master Server and Volume Server ###

The architecture is fairly simple. The actual data is stored in volumes on storage nodes. One volume server can have multiple volumes, and can both support read and write access with basic authentication.

All volumes are managed by a master server. The master server contains volume id to volume server mapping. This is fairly static information, and could be cached easily.

On each write request, the master server also generates a file key, which is a growing 64-bit unsigned integer. Since write requests are not generally as frequent as read requests, one master server should be able to handle the concurrency well.

### Write and Read files ###

When a client sends a write request, the master server returns (volume id, file key, file cookie, volume node url) for the file. The client then contacts the volume node and POSTs the file content.

When a client needs to read a file based on (volume id, file key, file cookie), it can ask the master server by the volume id for the (volume node url, volume node public url), or retrieve this from a cache. Then the client can GET the content, or just render the URL on web pages and let browsers fetch the content.

Please see the example for details on the write-read process.

### Storage Size ###

In the current implementation, each volume can be 8x2^32 bytes (32GiB). This is because of we align content to 8 bytes. We can easily increase this to 64G, or 128G, or more, by changing 2 lines of code, at the cost of some wasted padding space due to alignment.

There can be 2^32 volumes. So total system size is 8 x 2^32 bytes x 2^32 = 8 x 4GiB x 4Gi = 128EiB (2^67 bytes, or 128 exbibytes).

Each individual file size is limited to the volume size.

### Saving memory ###

All file meta information on volume server is readable from memory without disk access. Each file just takes an 16-byte map entry of <64bit key, 32bit offset, 32bit size>. Of course, each map entry has its own the space cost for the map. But usually the disk runs out before the memory does.


## Compared to Other File Systems ##

Most other distributed file systems seem more complicated than necessary.

### Compared to Ceph ###

Ceph can be setup similar to SeaweedFS as a key->blob store. It is much more complicated, with the need to support layers on top of it. [Here is a more detailed comparison](https://github.com/chrislusf/seaweedfs/issues/120)

SeaweedFS is meant to be fast and simple, both during usage and during setup. If you do not understand how it works when you reach here, we failed! Please raise an issue with any questions or update this file with clarifications.

SeaweedFS has a centralized master to look up free volumes, while Ceph uses hashing to locate its objects. Having a centralized master makes it easy to code and manage. HDFS/GFS has the single name node for years. SeaweedFS now support multiple master nodes.

Ceph hashing avoids SPOF, but makes it complicated when moving or adding servers.

### Compared to HDFS ###

HDFS uses the chunk approach for each file, and is ideal for streaming large files.

SeaweedFS is ideal for serving relatively smaller files quickly and concurrently.

SeaweedFS can also store extra large files by splitting them into manageable data chunks, and store the file ids of the data chunks into a meta chunk. This is managed by "weed upload/download" tool, and the weed master or volume servers are agnostic about it.

### Compared to MogileFS ###

SeaweedFS has 2 components: directory server, storage nodes.

MogileFS has 3 components: tracers, database, storage nodes.

One more layer means slower access, more operation complexity, more failure possibility.

### Compared to GlusterFS ###

SeaweedFS is not POSIX compliant, and has a simple implementation.

GlusterFS is POSIX compliant, and is much more complex.

### Compared to MongoDB's GridFS ###

Mongo's GridFS splits files into chunks and manage chunks in the central MongoDB. For every read or write request, the database needs to query the metadata. It's OK if this is not yet a bottleneck, but for a lot of concurrent reads this unnecessary query could slow things down.

Since files are chunked(default to 256KB), there will be multiple metadata readings and multiple chunk readings, linear to the file size. One 2.56MB file would require at least 20 disk read requests.

On the contrary, SeaweedFS uses large file volume of 32G size to store lots of files, and only manages file volumes in the master server. Each volume manages file metadata itself. So all file metadata is spread across the volume nodes, and just one disk read is needed.

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

go get github.com/chrislusf/seaweedfs/weed

once this is done, you should see the executable "weed" under $GOPATH/bin

step 4: after you modify your code locally, you could start a local build by calling "go install" under $GOPATH/src/github.com/chrislusf/seaweedfs/weed

## Disk Related topics ##

### Hard Drive Performance ###

When testing read performance on SeaweedFS, it basically becomes performance test for your hard drive's random read speed. Hard Drive usually get 100MB/s~200MB/s.

### Solid State Disk

To modify or delete small files, SSD must delete a whole block at a time, and move content in existing blocks to a new block. SSD is fast when brand new, but will get fragmented over time and you have to garbage collect, compacting blocks. SeaweedFS is friendly to SSD since it is append-only. Deletion and compaction are done on volume level in the background, not slowing reading and not causing fragmentation.

## Not Planned

POSIX support

## Benchmark

My Own Unscientific Single Machine Results on Mac Book with Solid State Disk, CPU: 1 Intel Core i7 2.6GHz.

Write 1 million 1KB file:
```
Concurrency Level:      16
Time taken for tests:   88.796 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1106764659 bytes
Requests per second:    11808.87 [#/sec]
Transfer rate:          12172.05 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.2      1.3       44.8      0.9

Percentage of the requests served within a certain time (ms)
   50%      1.1 ms
   66%      1.3 ms
   75%      1.5 ms
   80%      1.7 ms
   90%      2.1 ms
   95%      2.6 ms
   98%      3.7 ms
   99%      4.6 ms
  100%     44.8 ms
```

Randomly read 1 million files:
```
Concurrency Level:      16
Time taken for tests:   34.263 seconds
Complete requests:      1048576
Failed requests:        0
Total transferred:      1106762945 bytes
Requests per second:    30603.34 [#/sec]
Transfer rate:          31544.49 [Kbytes/sec]

Connection Times (ms)
              min      avg        max      std
Total:        0.0      0.5       20.7      0.7

Percentage of the requests served within a certain time (ms)
   50%      0.4 ms
   75%      0.5 ms
   95%      0.6 ms
   98%      0.8 ms
   99%      1.2 ms
  100%     20.7 ms
```


## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
