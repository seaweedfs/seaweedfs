
1. create "seaweedfs" database

export PGHOME=/Library/PostgreSQL/10
$PGHOME/bin/createdb --username=postgres --password seaweedfs

2. create "filemeta" table
$PGHOME/bin/psql --username=postgres --password seaweedfs

CREATE TABLE IF NOT EXISTS filemeta (
  dirhash     BIGINT,
  name        VARCHAR(65535),
  directory   VARCHAR(65535),
  meta        bytea,
  PRIMARY KEY (dirhash, name)
);

