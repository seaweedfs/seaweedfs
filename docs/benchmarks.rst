Benchmarks
======================

Do we really need the benchmark? People always use benchmark to compare systems. But benchmarks are misleading. The resources, e.g., CPU, disk, memory, network, all matter a lot. And with Weed File System, single node vs multiple nodes, benchmarking on one machine vs several multiple machines, all matter a lot.

Here is the steps on how to run benchmark if you really need some numbers.

Unscientific Single machine benchmarking
##################################################

I start weed servers in one console for simplicity. Better run servers on different consoles.

For more realistic tests, please start them on different machines.

.. code-block:: bash

  # prepare directories
  mkdir 3 4 5
  # start 3 servers
  ./weed server -dir=./3 -master.port=9333 -volume.port=8083 &
  ./weed volume -dir=./4 -port=8084 &
  ./weed volume -dir=./5 -port=8085 &
  ./weed benchmark -server=localhost:9333

What does the test do?
#############################

By default, the benchmark command would start writing 1 million files, each having 1KB size, uncompressed. For each file, one request is sent to assign a file key, and a second request is sent to post the file to the volume server. The written file keys are stored in a temp file.

Then the benchmark command would read the list of file keys, randomly read 1 million files. For each volume, the volume id is cached, so there is several request to lookup the volume id, and all the rest requests are to get the file content.

Many options are options are configurable. Please check the help content:

.. code-block:: bash

  ./weed benchmark -h

Common Problems
###############################

The most common problem is "too many open files" error. This is because the test itself starts too many network connections on one single machine. In my local macbook, if I ran "random read" following writing right away, the error happens always. I have to run "weed benchmark -write=false" to run the reading test only. Also, changing the concurrency level to "-c=16" would also help.

My own unscientific single machine results
###################################################

My Own Results on Mac Book with Solid State Disk, CPU: 1 Intel Core i7 at 2.2GHz.

.. code-block:: bash

  Write 1 million 1KB file:

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
  Randomly read 1 million files:

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

My own replication 001 single machine results
##############################################

Create benchmark volumes directly

.. code-block:: bash

  curl "http://localhost:9333/vol/grow?collection=benchmark&count=3&replication=001&pretty=y"
  # Later, after finishing the test, remove the benchmark collection
  curl "http://localhost:9333/col/delete?collection=benchmark&pretty=y"
  
  Write 1million 1KB files results:

  Concurrency Level:      64
  Time taken for tests:   174.949 seconds
  Complete requests:      1048576
  Failed requests:        0
  Total transferred:      1073741824 bytes
  Requests per second:    5993.62 [#/sec]
  Transfer rate:          5993.62 [Kbytes/sec]

  Connection Times (ms)
                min      avg        max      std
  Total:        0.3      10.4       296.6      4.4

  Percentage of the requests served within a certain time (ms)
     50%      9.7 ms
     66%     11.5 ms
     75%     12.1 ms
     80%     12.4 ms
     90%     13.4 ms
     95%     14.3 ms
     98%     15.5 ms
     99%     16.7 ms
    100%    296.6 ms
  Randomly read results:

  Concurrency Level:      64
  Time taken for tests:   53.987 seconds
  Complete requests:      1048576
  Failed requests:        0
  Total transferred:      1073741824 bytes
  Requests per second:    19422.81 [#/sec]
  Transfer rate:          19422.81 [Kbytes/sec]

  Connection Times (ms)
                min      avg        max      std
  Total:        0.0      3.0       256.9      3.8

  Percentage of the requests served within a certain time (ms)
     50%      2.7 ms
     66%      2.9 ms
     75%      3.2 ms
     80%      3.5 ms
     90%      4.4 ms
     95%      5.6 ms
     98%      7.4 ms
     99%      9.4 ms
    100%    256.9 ms
How can the replication 001 writes faster than no replication?
I could not tell. Very likely, the computer was in turbo mode. I can not reproduce it consistently either. Posted the number here just to illustrate that number lies. Don't quote on the exact number, just get an idea of the performance would be good enough.