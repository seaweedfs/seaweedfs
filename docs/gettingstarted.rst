Getting started
===================================
Installing Seaweed-FS
###################################

Download a proper version from  `Seaweed-FS download page <https://bintray.com/chrislusf/Weed-FS/weed/>`_.

Decompress the downloaded file. You will only find one executable file, either "weed" on most systems or "weed.exe" on windows.

Put the file "weed" to all related computers, in any folder you want. Use 

.. code-block:: bash

	./weed -h # to check available options


Set up Weed Master
*********************************

.. code-block:: bash

	./weed master -h # to check available options

If no replication is required, this will be enough. The "mdir" option is to configure a folder where the generated sequence file ids are saved.

.. code-block:: bash

	./weed master -mdir="."


If you need replication, you would also set the configuration file. By default it is "/etc/weedfs/weedfs.conf" file. The example can be found in RackDataCenterAwareReplication

Set up Weed Volume Server
*********************************

.. code-block:: bash

	./weed volume -h # to check available options

Usually volume servers are spread on different computers. They can have different disk space, or even different operating system.

Usually you would need to specify the available disk space, the Weed Master location, and the storage folder.

.. code-block:: bash

	./weed volume -max=100 -mserver="localhost:9333" -dir="./data"

Cheat Sheet: Setup One Master Server and One Volume Server
**************************************************************

Actually, forget about previous commands. You can setup one master server and one volume server in one shot:

.. code-block:: bash

	./weed server -dir="./data"
	# same, just specifying the default values
	# use "weed server -h" to find out more
	./weed server -master.port=9333 -volume.port=8080 -dir="./data"

Testing Seaweed-FS
###################################

With the master and volume server up, now what? Let's pump in a lot of files into the system!

.. code-block:: bash
	
	./weed upload -dir="/some/big/folder"

This command would recursively upload all files. Or you can specify what files you want to include.


.. code-block:: bash

	./weed upload -dir="/some/big/folder" -include=*.txt

Then, you can simply check "du -m -s /some/big/folder" to see the actual disk usage by OS, and compare it with the file size under "/data". Usually if you are uploading a lot of textual files, the consumed disk size would be much smaller since textual files are gzipped automatically.

Now you can use your tools to hit weed-fs as hard as you can.

Using Seaweed-FS in docker
####################################

You can use image "cydev/weed" or build your own with  `dockerfile <https://github.com/chrislusf/weed-fs/blob/master/Dockerfile>`_  in the root of repo.

Using pre-built Docker image
**************************************************************

.. code-block:: bash

	docker run --name weed cydev/weed server

And in another terminal

.. code-block:: bash

	IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' weed)
	curl "http://$IP:9333/cluster/status?pretty=y"	
	{
  		"IsLeader": true,
  		"Leader": "localhost:9333"
	}
	# use $IP as host for api queries

Building image from dockerfile
**************************************************************

Make a local copy of weed-fs from github

.. code-block:: bash

	git clone https://github.com/chrislusf/weed-fs.git

Minimal Image (~19.6 MB)

.. code-block:: bash

	docker build --no-cache --rm -t 'cydev/weed' .

Go-Build Docker Image (~764 MB)

.. code-block:: bash

	mv Dockerfile Dockerfile.minimal
	mv Dockerfile.go_build Dockerfile
	docker build --no-cache --rm -t 'cydev/weed' .

In production
**************************************************************

To gain persistency you can use docker volumes.

.. code-block:: bash

	# start our weed server daemonized
	docker run --name weed -d -p 9333:9333 -p 8080:8080 \
		-v /opt/weedfs/data:/data cydev/weed server -dir="/data" \ 
		-publicIp="$(curl -s cydev.ru/ip)"

Now our weed-fs server will be persistent and accessible by localhost:9333 and :8080 on host machine.
Dont forget to specify "-publicIp" for correct connectivity.
