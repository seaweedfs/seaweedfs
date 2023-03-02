# SEAWEEDFS - helm chart (2.x+)

## Getting Started

### Add the helm repo

`helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm`

### Install the helm chart

`helm install seaweedfs seaweedfs/seaweedfs`

### (Recommended) Provide `values.yaml`

`helm install --values=values.yaml seaweedfs seaweedfs/seaweedfs`

## Info:
* master/filer/volume are stateful sets with anti-affinity on the hostname,
so your deployment will be spread/HA.
* chart is using memsql(mysql) as the filer backend to enable HA (multiple filer instances) and backup/HA memsql can provide.
* mysql user/password are created in a k8s secret (secret-seaweedfs-db.yaml) and injected to the filer with ENV.
* cert config exists and can be enabled, but not been tested, requires cert-manager to be installed.

## Prerequisites
### Database

leveldb is the default database this only supports one filer replica.

To have multiple filers a external datastore is recommened.

Such as MySQL-compatible database, as specified in the `values.yaml` at `filer.extraEnvironmentVars`. 
This database should be pre-configured and initialized by running:
```sql
CREATE TABLE IF NOT EXISTS `filemeta` (
  `dirhash`   BIGINT NOT NULL       COMMENT 'first 64 bits of MD5 hash value of directory field',
  `name`      VARCHAR(766) NOT NULL COMMENT 'directory or file name',
  `directory` TEXT NOT NULL         COMMENT 'full path to parent directory',
  `meta`      LONGBLOB,
  PRIMARY KEY (`dirhash`, `name`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

Alternative database can also be configured (e.g. leveldb, postgres) following the instructions at `filer.extraEnvironmentVars`.

### Node Labels
Kubernetes nodes can have labels which help to define which node(Host) will run which pod:

Here is an example:
* s3/filer/master needs the label **sw-backend=true**
* volume need the label **sw-volume=true**

to label a node to be able to run all pod types in k8s:
```
kubectl label node YOUR_NODE_NAME sw-volume=true,sw-backend=true
```

on production k8s deployment you will want each pod to have a different host,
especially the volume server and the masters, all pods (master/volume/filer)
should have anti-affinity rules to disallow running multiple component pods  on the same host.

If you still want to run multiple pods of the same component (master/volume/filer) on the same host please set/update the corresponding affinity rule in values.yaml to an empty one:

```affinity: ""```

## PVC - storage class ###

On the volume stateful set added support for k8s PVC, currently example
with the simple local-path-provisioner from Rancher (comes included with k3d / k3s)
https://github.com/rancher/local-path-provisioner

you can use ANY storage class you like, just update the correct storage-class
for your deployment.

## current instances config (AIO):

1 instance for each type (master/filer+s3/volume)

You can update the replicas count for each node type in values.yaml,
need to add more nodes with the corresponding labels if applicable.

Most of the configuration are available through values.yaml any pull requests to expand functionality or usability are greatly appreciated. Any pull request must pass [chart-testing](https://github.com/helm/chart-testing).