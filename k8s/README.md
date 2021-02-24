## SEAWEEDFS - helm chart (2.x)

### info:
* master/filer/volume are stateful sets with anti-affinity on the hostname,
so your deployment will be spread/HA.
* chart is using memsql(mysql) as the filer backend to enable HA (multiple filer instances)
and backup/HA memsql can provide.
* mysql user/password are created in a k8s secret (secret-seaweedfs-db.yaml) and injected to the filer
with ENV.
* cert config exists and can be enabled, but not been tested.

### prerequisites
kubernetes node have labels which help to define which node(Host) will run which pod.

s3/filer/master needs the label **sw-backend=true**

volume need the label **sw-volume=true**

to label a node to be able to run all pod types in k8s:
```
kubectl label node YOUR_NODE_NAME sw-volume=true,sw-backend=true
```

on production k8s deployment you will want each pod to have a different host,
especially the volume server & the masters, currently all pods (master/volume/filer)
have anti-affinity rule to disallow running multiple pod type on the same host.
if you still want to run multiple pods of the same type (master/volume/filer) on the same host
please set/update the corresponding affinity rule in values.yaml to an empty one:

```affinity: ""```

### PVC - storage class ###

on the volume stateful set added support for K8S PVC, currently example
with the simple local-path-provisioner from Rancher (comes included with k3d / k3s)
https://github.com/rancher/local-path-provisioner

you can use ANY storage class you like, just update the correct storage-class
for your deployment.

### current instances config (AIO):
1 instance for each type (master/filer+s3/volume)

you can update the replicas count for each node type in values.yaml,
need to add more nodes with the corresponding labels.

most of the configuration are available through values.yaml

