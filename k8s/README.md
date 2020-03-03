## SEAWEEDFS - helm chart (2.x)

### info:
* master/filer/volume are stateful sets with anti-affinity on the hostname,
so your deployment will be spread/HA.
* chart is using memsql(mysql) as the filer backend to enable HA (multiple filer instances)
and backup/HA memsql can provide.
* mysql user/password are created in a k8s secret (secret-seaweedfs-db.yaml) and injected to the filer
with ENV.
* cert config exists and can be enabled, but not been tested.

### current instances config (AIO):
1 instance for each type (master/filer/volume/s3)

instances need node labels:
* sw-volume: true  (for volume instance, specific tag)
* sw-backend: true (for all others, as they less resource demanding)

you can update the replicas count for each node type in values.yaml,
need to add more nodes with the corresponding label.

most of the configuration are available through values.yaml

