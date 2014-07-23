Failover master server
=======================


Introduction
###################################

Some user will ask for no single point of failure. Although google runs its file system with a single master for years, no SPOF seems becoming a criteria for architects to pick solutions.

Luckily, it's not too difficult to enable Weed File System with failover master servers.

Cheat Sheet: Startup multiple servers
########################################

This section is a quick way to start 3 master servers and 3 volume servers. All done!

.. code-block:: bash

	weed server -master.port=9333 -dir=./1 -volume.port=8080 \ 
		-master.peers=localhost:9333,localhost:9334,localhost:9335
	weed server -master.port=9334 -dir=./2 -volume.port=8081 \ 
		-master.peers=localhost:9333,localhost:9334,localhost:9335
	weed server -master.port=9335 -dir=./3 -volume.port=8082 \ 
		-master.peers=localhost:9333,localhost:9334,localhost:9335

Or, you can use this "-peers" settings to add master servers one by one.

.. code-block:: bash

	weed server -master.port=9333 -dir=./1 -volume.port=8080
	weed server -master.port=9334 -dir=./2 -volume.port=8081 -master.peers=localhost:9333
	weed server -master.port=9335 -dir=./3 -volume.port=8082 -master.peers=localhost:9334

How it works
##########################

The master servers are coordinated by Raft protocol, to elect a leader. The leader took over all the work to manage volumes, assign file ids. All other master servers just simply forward requests to the leader.

If the leader dies, another leader will be elected. And all the volume servers will send their heartbeat together with their volumes information to the new leader. The new leader will take the full responsibility.

During the transition, there could be moments where the new leader has partial information about all volume servers. This just means those yet-to-heartbeat volume servers will not be writable temporarily.

Startup multiple master servers
###############################################

Now let's start the master and volume servers separately, the usual way.

Usually you would start several (3 or 5) master servers, then start the volume servers:

.. code-block:: bash

	weed master -port=9333 -mdir=./1
	weed master -port=9334 -mdir=./2 -peers=localhost:9333
	weed master -port=9335 -mdir=./3 -peers=localhost:9334
	# now start the volume servers, specifying any one of the master server
	weed volume -dir=./1 -port=8080
	weed volume -dir=./2 -port=8081 -mserver=localhost:9334
	weed volume -dir=./3 -port=8082 -mserver=localhost:9335

These 6 commands will actually functioning the same as the previous 3 commands from the cheatsheet.

Even though we only specified one peer in "-peers" option to bootstrap, the master server will get to know all the other master servers in the cluster, and store these information in the local directory.

When you need to restart
If you need to restart the master servers, just run the master servers WITHOUT the "-peers" option.

.. code-block:: bash

	weed master -port=9333 -mdir=./1
	weed master -port=9334 -mdir=./2
	weed master -port=9335 -mdir=./3

To understand why, remember that the cluster information is "sticky", meaning it is stored on disk. If you restart the server, the cluster information stay the same, so the "-peers" option is not needed again.

Common Problem
############################

This "sticky" cluster information can cause some misunderstandings. For example, here is one:

https://code.google.com/p/weed-fs/issues/detail?id=70

The previously used value "localhost" would come up even not specified. This could cause your some time to figure out.