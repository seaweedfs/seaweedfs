Optimization
==============

Here are the strategies or best ways to optimize WeedFS.

Increase concurrent writes
################################

By default, WeedFS grows the volumes automatically. For example, for no-replication volumes, there will be concurrently 7 writable volumes allocated.

If you want to distribute writes to more volumes, you can do so by instructing WeedFS master via this URL.

.. code-block:: bash
	
	curl http://localhost:9333/vol/grow?count=12&replication=001

This will assign 12 volumes with 001 replication. Since 001 replication means 2 copies for the same data, this will actually consumes 24 physical volumes.

Increase concurrent reads
################################

Same as above, more volumes will increase read concurrency.

In addition, increase the replication will also help. Having the same data stored on multiple servers will surely increase read concurrency.

Add more hard drives
################################

More hard drives will give you better write/read throughput.

Gzip content
################################

WeedFS determines the file can be gzipped based on the file name extension. So if you submit a textual file, it's better to use an common file name extension, like ".txt", ".html", ".js", ".css", etc. If the name is unknown, like ".go", WeedFS will not gzip the content, but just save the content as is.

You can also manually gzip content before submission. If you do so, make sure the submitted file has file name with ends with ".gz". For example, "my.css" can be gzipped to "my.css.gz" and sent to WeedFS. When retrieving the content, if the http client supports "gzip" encoding, the gzipped content would be sent back. Otherwise, the unzipped content would be sent back.

Memory consumption
#################################

For volume servers, the memory consumption is tightly related to the number of files. For example, one 32G volume can easily have 1.5 million files if each file is only 20KB. To store the 1.5 million entries of meta data in memory, currently WeedFS consumes 36MB memory, about 24bytes per entry in memory. So if you allocate 64 volumes(2TB), you would need 2~3GB memory. However, if the average file size is larger, say 200KB, only 200~300MB memory is needed.

Theoretically the memory consumption can go even lower by compacting since the file ids are mostly monotonically increasing. I did not invest time on that yet since the memory consumption, 24bytes/entry(including uncompressed 8bytes file id, 4 bytes file size, plus additional map data structure cost) is already pretty low. But I welcome any one to compact these data in memory even more efficiently.

Insert with your own keys
################################

The file id generation is actually pretty trivial and you could use your own way to generate the file keys.

A file key has 3 parts:

* volume id: a volume with free spaces
* file id: a monotonously increasing and unique number
* file cookie: a random number, you can customize it in whichever way you want

You can directly ask master server to assign a file key, and replace the file id part to your own unique id, e.g., user id.

Also you can get each volume's free space from the server status.

.. code-block:: bash
	
	curl "http://localhost:9333/dir/status?pretty=y"

Once you are sure about the volume free spaces, you can use your own file ids. Just need to ensure the file key format is compatible.

The assigned file cookie can also be customized.

Customizing the file id and/or file cookie is an acceptable behavior. "strict monotonously increasing" is not necessary, but keeping file id in a "mostly" increasing order is expected in order to keep the in memory data structure efficient.

Upload large files
###################################

If files are large and network is slow, the server will take time to read the file. Please increase the "-readTimeout=3" limit setting for volume server. It cut off the connection if uploading takes a longer time than the limit.

Upload large files with Auto Split/Merge
If the file is large, it's better to upload this way:

.. code-block:: bash

  weed upload -maxMB=64 the_file_name

This will split the file into data chunks of 64MB each, and upload them separately. The file ids of all the data chunks are saved into an additional meta chunk. The meta chunk's file id are returned.

When downloading the file, just

.. code-block:: bash

  weed download the_meta_chunk_file_id

The meta chunk has the list of file ids, with each file id on each line. So if you want to process them in parallel, you can download the meta chunk and deal with each data chunk directly.

Collection as a Simple Name Space
When assigning file ids,

.. code-block:: bash

	curl http://master:9333/dir/assign?collection=pictures
	curl http://master:9333/dir/assign?collection=documents

will also generate a "pictures" collection and a "documents" collection if they are not created already. Each collection will have its dedicated volumes, and they will not share the same volume.

Actually, the actual data files have the collection name as the prefix, e.g., "pictures_1.dat", "documents_3.dat".

In case you need to delete them later, you can go to the volume servers and delete the data files directly, for now. Later maybe a deleteCollection command may be implemented, if someone asks...

Logging
##############################

When going to production, you will want to collect the logs. WeedFS uses glog. Here are some examples:

.. code-block:: bash

	weed -v=2 master
	weed -log_dir=. volume