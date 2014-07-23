Replication
===================================
Weed-FS can support replication. The replication is implemented not on file level, but on volume level.

How to use
###################################
Basically, the way it works is:

1. start weed master, and optionally specify the default replication type

.. code-block:: bash
  
  ./weed master -defaultReplicationType=001

2. start volume servers as this:

.. code-block:: bash

   ./weed volume -port=8081 -dir=/tmp/1 -max=100
   ./weed volume -port=8082 -dir=/tmp/2 -max=100
   ./weed volume -port=8083 -dir=/tmp/3 -max=100

Submitting, Reading, Deleting files has the same steps.

The meaning of replication type
###################################
*Note: This subject to change.*

+-----+---------------------------------------------------------------------------+
|000  |no replication, just one copy                                              |
+-----+---------------------------------------------------------------------------+
|001  |replicate once on the same rack                                            |
+-----+---------------------------------------------------------------------------+
|010  |replicate once on a different rack in the same data center                 |
+-----+---------------------------------------------------------------------------+
|100  |replicate once on a different data center                                  |
+-----+---------------------------------------------------------------------------+
|200  |replicate twice on two other different data center                         |
+-----+---------------------------------------------------------------------------+
|110  |replicate once on a different rack, and once on a different data center    |
+-----+---------------------------------------------------------------------------+
|...  |...                                                                        |
+-----+---------------------------------------------------------------------------+

So if the replication type is xyz

+-------+--------------------------------------------------------+
|**x**  |number of replica in other data centers                 |
+-------+--------------------------------------------------------+
|**y**  |number of replica in other racks in the same data center|
+-------+--------------------------------------------------------+
|**z**  |number of replica in other servers in the same rack     |
+-------+--------------------------------------------------------+

x,y,z each can be 0, 1, or 2. So there are 9 possible replication types, and can be easily extended. 
Each replication type will physically create x+y+z+1 copies of volume data files.

Example topology configuration 
###################################

The WeedFS master server tries to read the default topology configuration file are read from /etc/weedfs/weedfs.conf, if it exists. The topology setting to configure data center and racks file format is as this.

.. code-block:: xml

  <Configuration>
    <Topology>
      <DataCenter name="dc1">
        <Rack name="rack1">
          <Ip>192.168.1.1</Ip>
        </Rack>
      </DataCenter>
      <DataCenter name="dc2">
        <Rack name="rack1">
          <Ip>192.168.1.2</Ip>
        </Rack>
        <Rack name="rack2">
          <Ip>192.168.1.3</Ip>
          <Ip>192.168.1.4</Ip>
        </Rack>
      </DataCenter>
    </Topology>
  </Configuration>

Allocate File Key on specific data center
Volume servers can start with a specific data center name.

.. code-block:: bash

  weed volume -dir=/tmp/1 -port=8080 -dataCenter=dc1
  weed volume -dir=/tmp/2 -port=8081 -dataCenter=dc2

Or the master server can determine the data center via volume server's IP address and settings in weed.conf file.

Now when requesting a file key, an optional "dataCenter" parameter can limit the assigned volume to the specific data center. For example, this specify

.. code-block:: bash
  
  http://localhost:9333/dir/assign?dataCenter=dc1