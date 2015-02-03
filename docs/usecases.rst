Use cases
===================

Saving image with different sizes
#############################

Each image usually store one file key in database. However, one image can have several versions, e.g., thumbnail, small, medium, large, original. And each version of the same image will have a file key. It's not ideal to store all the keys.

One way to resolve this is here.

Reserve a set of file keys, for example, 5

.. code-block:: bash

	curl http://<host>:<port>/dir/assign?count=5
	{"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080","count":5}

Save the 5 versions of the image to the volume server. The urls for each image can be:

.. code-block:: bash

	http://<url>:<port>/3,01637037d6
	http://<url>:<port>/3,01637037d6_1
	http://<url>:<port>/3,01637037d6_2
	http://<url>:<port>/3,01637037d6_3
	http://<url>:<port>/3,01637037d6_4

Overwriting mime types
#############################

The correct way to send mime type:


.. code-block:: bash

	curl -F "file=@myImage.png;type=image/png" http://127.0.0.1:8081/5,2730a7f18b44

The wrong way to send it:

.. code-block:: bash

	curl -H "Content-Type:image/png" -F file=@myImage.png http://127.0.0.1:8080/5,2730a7f18b44
	
Securing Seaweed-FS
#############################

The simple way is to front all master and volume servers with firewall.

However, if blocking servicing port is not feasible or trivial, a white list option can be used. Only traffic from the white list IP addresses have write permission.

.. code-block:: bash

	weed master -whiteList="::1,127.0.0.1"
	weed volume -whiteList="::1,127.0.0.1"
	# "::1" is for IP v6 localhost.


Data Migration Example
#############################

.. code-block:: bash

	weed master -mdir="/tmp/mdata" -defaultReplication="001" -ip="localhost" -port=9334
	weed volume -dir=/tmp/vol1/ -mserver="localhost:9334" -ip="localhost" -port=8081
	weed volume -dir=/tmp/vol2/ -mserver="localhost:9334" -ip="localhost" -port=8082
	weed volume -dir=/tmp/vol3/ -mserver="localhost:9334" -ip="localhost" -port=8083

.. code-block:: bash

	ls vol1 vol2 vol3
	vol1:
	1.dat 1.idx 2.dat 2.idx 3.dat 3.idx 5.dat 5.idx
	vol2:
	2.dat 2.idx 3.dat 3.idx 4.dat 4.idx 6.dat 6.idx
	vol3:
	1.dat 1.idx 4.dat 4.idx 5.dat 5.idx 6.dat 6.idx

stop all of them

move vol3/* to vol1 and vol2

it is ok to move x.dat and x.idx from one volumeserver to another volumeserver, 
because they are exactly the same. 
it can be checked by md5.

.. code-block:: bash

	md5 vol1/1.dat vol2/1.dat
	MD5 (vol1/1.dat) = c1a49a0ee550b44fef9f8ae9e55215c7
	MD5 (vol2/1.dat) = c1a49a0ee550b44fef9f8ae9e55215c7
	md5 vol1/1.idx vol2/1.idx
	MD5 (vol1/1.idx) = b9edc95795dfb3b0f9063c9cc9ba8095
	MD5 (vol2/1.idx) = b9edc95795dfb3b0f9063c9cc9ba8095

.. code-block:: bash

	ls vol1 vol2 vol3
	vol1:
	1.dat 1.idx 2.dat 2.idx 3.dat 3.idx 4.dat 4.idx 5.dat 5.idx 6.dat 6.idx
	vol2:
	1.dat 1.idx 2.dat 2.idx 3.dat 3.idx 4.dat 4.idx 5.dat 5.idx 6.dat 6.idx
	vol3:

start

.. code-block:: bash

	weed master -mdir="/tmp/mdata" -defaultReplication="001" -ip="localhost" -port=9334
	weed volume -dir=/tmp/vol1/ -mserver="localhost:9334" -ip="localhost" -port=8081
	weed volume -dir=/tmp/vol2/ -mserver="localhost:9334" -ip="localhost" -port=8082

so we finished moving data of localhost:8083 to localhost:8081/localhost:8082 


