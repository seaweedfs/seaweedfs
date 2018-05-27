1. create a keyspace

CREATE KEYSPACE seaweedfs WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

2. create filemeta table

 CREATE TABLE filemeta (
    directory varchar,
    name varchar,
    meta blob,
    PRIMARY KEY (directory, name)
 ) WITH CLUSTERING ORDER BY (name ASC);
