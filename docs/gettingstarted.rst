Getting started
===================================
Installing Weed-Fs
###################################

Download a proper version from  `WeedFS download page <https://bintray.com/chrislusf/Weed-FS/weed/>`_.

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

Testing Weed-Fs
###################################

With the master and volume server up, now what? Let's pump in a lot of files into the system!

.. code-block:: bash
	
	./weed upload -dir="/some/big/folder"

This command would recursively upload all files. Or you can specify what files you want to include.


.. code-block:: bash

	./weed upload -dir="/some/big/folder" -include=*.txt

Then, you can simply check "du -m -s /some/big/folder" to see the actual disk usage by OS, and compare it with the file size under "/data". Usually if you are uploading a lot of textual files, the consumed disk size would be much smaller since textual files are gzipped automatically.

Now you can use your tools to hit weed-fs as hard as you can.
