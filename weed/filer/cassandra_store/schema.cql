/*

Here is the CQL to create the table.CassandraStore

Optionally you can adjust the keyspace name and replication settings.

For production server, very likely you want to set replication_factor to 3

*/

create keyspace seaweed WITH replication = {
  'class':'SimpleStrategy',
  'replication_factor':1
};

use seaweed;

CREATE TABLE seaweed_files (
   path varchar,
   fids list<varchar>,
   PRIMARY KEY (path)
);
