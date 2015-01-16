Change List
===================================

Introduction
############
This file contains list of recent changes, important features, usage changes, data format changes, etc. Do read this if you upgrade.

v0.68
#####
1. Filer supports storing file~file_id mapping to remote key-value storage Redis, Cassandra. So multiple filers are supported.

v0.67
#####
1. Increase "weed benchmark" performance to pump in more data. The bottleneck is on the client side. Duh...

v0.65
#####

1. Reset the cluster configuration if "-peers" is not empty.

v0.64
#####

1. Add TTL support!
1. filer: resolve directory log file error, avoid possible race condition

v0.63
#####

1. Compiled with Go 1.3.1 to fix a rare crashing issue.

v0.62
#####

1. Add support for Etag.
2. Add /admin/mv to move a file or a folder.
3. Add client Go API to pre-process the images.

v0.61
#####

1. Reduce memory requirements for "weed fix"
2. Guess mime type by file name extensions when stored mime type is "application/octstream"
3. Added simple volume id lookup caching expiring by time.

v0.60
#####

Fix file missing error caused by .idx file overwriting. The problem shows up if the weed volume server is restarted after 2 times. But the actual .idx file may have already been overwritten on second restart.

To fix this issue, please run "weed fix -dir=... -volumeId=..." to re-generate the .idx file.

v0.59
#####

1. Add option to automatically fix jpeg picture orientation.
2. Add volume id lookup caching
3. Support Partial Content and Range Requests. http status code == 206.

v0.57
#####

Add hidden dynamic image resizing feature

Add an hidden feature: For images, jpg/png/gif, if you specify append these url parameters, &width=xxx or &height=xxx or both, the image will be dynamically resized. However, resizing the image would cause high CPU and memory usage. Not recommended unless special use cases. So this would not be documented anywhere else.

v0.56 Major Command line options change
#####


Adjust command line options.

1. switch to use -publicIp instead of -publicUrl
2. -ip can be empty. It will listen to all available interfaces.
3. For "weed server", these options are changed:
  - -masterPort => -master.port
  - -peers => -master.peers
  - -mdir => -master.dir
  - -volumeSizeLimitMB => -master.volumeSizeLimitMB
  - -conf => -master.conf
  - -defaultReplicaPlacement => -master.defaultReplicaPlacement
  - -port => -volume.port
  - -max => -volume.max

v0.55 Recursive folder deletion for Filer
#####

Now folders with sub folders or files can be deleted recursively.

Also, for filer, avoid showing files under the first created directory when listing the root directory.

v0.54 Misc improvements
#####

No need to persist metadata for master sequence number generation. This shall avoid possible issues where file are lost due to duplicated sequence number generated in rare cases.

More robust handing of "peers" in master node clustering mode.

Added logging instructions.

v0.53  Miscellaneous improvements
#####

Added retry logic to wait for cluster peers during cluster bootstrapping. Previously the cluster bootstrapping is ordered. This make it tricky to deploy automatically and repeatedly. The fix make the commands repeatable.

Also, when growing volumes, additional preferred "rack" and "dataNode" parameters are also provided, works together with existing "dataCenter" parameter.

Fix important bug where settings for non-"000" replications are read back wrong, if volume server is restarted.

v0.52 Added "filer" server
#####

A "weed filer" server is added, to provide more "common" file storage. Currently the fullFileName-to-fileId mapping is stored with an efficient embedded leveldb. So it's not linearly scalable yet. But it can handle LOTS of files.


.. code-block:: bash

  //POST a file and read it back
  curl -F "filename=@README.md" "http://localhost:8888/path/to/sources/"
  curl "http://localhost:8888/path/to/sources/README.md"
  //POST a file with a new name and read it back
  curl -F "filename=@Makefile" "http://localhost:8888/path/to/sources/new_name"
  curl "http://localhost:8888/path/to/sources/new_name"
  //list sub folders and files
  curl "http://localhost:8888/path/to/sources/?pretty=y"


v0.51 Idle Timeout
#####

Previously the timeout setting is "-readTimeout", which is the time limit of the whole http connection. This is inconvenient for large files or for slow internet connections.  Now this option is replaced with "-idleTimeout", and default to 10 seconds. Ideally, you should not need to tweak it based on your use case.

v0.50 Improved Locking
#####

1. All read operation switched to thread-safe pread, no read locks now.
2. When vacuuming large volumes, a lock was preventing heartbeats to master node. This is fixed now.
3. Fix volume compaction error for collections.

v0.49 Bug Fixes
#####

With the new benchmark tool to bombard the system, many bugs are found and fixed, especially on clustering, http connection reuse.

v0.48 added benchmark command!
#####

Benchmark! Enough said.

v0.47 Improving replication
#####

Support more replication types.

v0.46 Adding failover master server
#####

Automatically fail over master servers!

v0.46 Add "weed server" command
#####

Now you can start one master server and one volume server in just one command!

.. code-block:: bash

 weed server


v0.45 Add support for extra large file
#####

For extra large file, this example will split the file into 100MB chunks.

.. code-block:: bash

 weed upload -maxMB=100 the_file_name


Also, Added "download" command, for simple files or chunked files.

.. code-block:: bash

 weed download file_id [file_id3](file_id2)


v0.34 Add support for multiple directories on volume server
#####

For volume server, add support for multiple folders and multiple max limit. For example:

.. code-block:: bash

 weed volume -dir=folder1,folder2,folder3 -max=7,8,9


v0.33 Add Nicer URL support
#####

For HTTP GET request

.. code-block:: bash

  http://localhost:8080/3,01637037d6

Can also be retrieved by

.. code-block:: bash

  http://localhost:8080/3/01637037d6/my_preferred_name.jpg


v0.32 Add support for Last-Modified header
#####

The last modified timestamp is stored with 5 additional bytes.

Return http code 304 if the file is not modified.

Also, the writing are more solid with the fix for issue#26.

v0.31 Allocate File Key on specific data center
#####

Volume servers can start with a specific data center name.

.. code-block:: bash

 weed volume -dir=/tmp/1 -port=8080 -dataCenter=dc1
 weed volume -dir=/tmp/2 -port=8081 -dataCenter=dc2

Or the master server can determine the data center via volume server's IP address and settings in weed.conf file.

Now when requesting a file key, an optional "dataCenter" parameter can limit the assigned volume to the specific data center. For example, this specif

.. code-block:: bash

 http://localhost:9333/dir/assign?dataCenter=dc1

v0.26 Storing File Name and Mime Type
#####

In order to keep one single disk read for each file, a new storage format is implemented to store: is gzipped or not, file name and mime type (used when downloading files), and possibly other future new attributes.  The volumes with old storage format are treated as read only and deprecated.

Also, you can pre-gzip and submit your file directly, for example, gzip "my.css" into "my.css.gz", and submit. In this case, "my.css" will be stored as the file name. This should save some transmission time, and allow you to force gzipped storage or customize the gzip compression level.

v0.25 Adding reclaiming garbage spaces

Garbage spaces are reclaimed by an automatic compacting process. Garbage spaces are generated when updating or deleting files. If they exceed a configurable threshold, 0.3 by default (meaning 30% of the used disk space is garbage), the volume will be marked as readonly, compacted and garbage spaces are reclaimed, and then marked as writable.

v0.19 Adding rack and data center aware replication
#####

Now when you have one rack, or multiple racks, or multiple data centers, you can choose your own replication strategy.

v0.18 Detect disconnected volume servers
#####

The disconnected volume servers would not be assigned when generating the file keys. Volume servers by default send a heartbeat to master server every 5~10 seconds. Master thinks the volume server is disconnected after 5 times of the heartbeat interval, or 25 seconds by default.

v0.16 Change to single executable file to do everything
#####

If you are using v0.15 or earlier, you would use

.. code-block:: bash

  >weedvolume -dir="/tmp" -volumes=0-4 -mserver="localhost:9333" -port=8080 -publicUrl="localhost:8080"

With v0.16 or later, you would need to do this in stead:

.. code-block:: bash

  >weed volume -dir="/tmp" -volumes=0-4 -mserver="localhost:9333" -port=8080 -publicUrl="localhost:8080"

And more new commands, in addition to "server","volume","fix", etc, will be added.

This provides a simple deliverable file, and the file size is much smaller since Go language statically compile the commands. Combining commands into one file would avoid lots of duplication.
