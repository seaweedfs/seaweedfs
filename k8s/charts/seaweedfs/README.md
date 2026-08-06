# SEAWEEDFS - helm chart (2.x+)


### Add the helm repo

```bash
helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm
```

### Install the helm chart

```bash
helm install seaweedfs seaweedfs/seaweedfs
```

### (Recommended) Provide `values.yaml`

```bash
helm install --values=values.yaml seaweedfs seaweedfs/seaweedfs
```

## Info:
* master/filer/volume are stateful sets with anti-affinity on the hostname,
so your deployment will be spread/HA.
* chart is using memsql(mysql) as the filer backend to enable HA (multiple filer instances) and backup/HA memsql can provide.
* mysql user/password are created in a k8s secret (default: `<release>-seaweedfs-db-secret`) and injected to the filer with ENV.
* cert config exists and can be enabled, but not been tested, requires cert-manager to be installed.

## Prerequisites
### Database

leveldb is the default database, this supports multiple filer replicas that will [sync automatically](https://github.com/seaweedfs/seaweedfs/wiki/Filer-Store-Replication), with some [limitations](https://github.com/seaweedfs/seaweedfs/wiki/Filer-Store-Replication#limitation).

When the [limitations](https://github.com/seaweedfs/seaweedfs/wiki/Filer-Store-Replication#limitation) apply, or for a large number of filer replicas, an external datastore is recommended.

Such as MySQL-compatible database, as specified in the `values.yaml` at `filer.extraEnvironmentVars`.
This database should be pre-configured and initialized. If using the default `db-init-config`, the configmap name is now dynamic (e.g., `<release>-seaweedfs-db-init-config`). You can override this name via `filer.dbInitConfigName`.

To initialize manually:
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

#### RocksDB variant

The `_large_disk_rocksdb` image tag ships with RocksDB pre-configured as the filer backend.
To use this image with the Helm chart, override the image on all three components and disable
the chart's default `WEED_LEVELDB2_ENABLED`, which would otherwise re-enable LevelDB2 and
override the image's built-in RocksDB configuration:

```yaml
# Replace <VERSION> with the desired seaweedfs version, e.g. 3.80_large_disk_rocksdb.
master:
  imageOverride: chrislusf/seaweedfs:<VERSION>_large_disk_rocksdb

volume:
  imageOverride: chrislusf/seaweedfs:<VERSION>_large_disk_rocksdb

filer:
  enablePVC: true
  imageOverride: chrislusf/seaweedfs:<VERSION>_large_disk_rocksdb
  extraEnvironmentVars:
    WEED_LEVELDB2_ENABLED: "false"
```

Notes:

* `master` and `volume` use the same image tag so that all components share a consistent
  SeaweedFS build; RocksDB itself is only used by the filer.
* `filer.enablePVC: true` (or another form of persistent storage for the filer) is required
  so that the RocksDB metadata store survives pod restarts — otherwise metadata will be lost.

### Node Labels
Kubernetes nodes can have labels which help to define which node(Host) will run which pod:

Here is an example:
* s3/filer/master needs the label **sw-backend=true**
* volume need the label **sw-volume=true**

to label a node to be able to run all pod types in k8s:
```
kubectl label node YOUR_NODE_NAME sw-volume=true sw-backend=true
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

### Master data: hostPath vs a claim

The master's `-mdir` holds its Raft log and snapshots, and with them the
cluster's identity (its topology UUID). `master.data.type` defaults to
`hostPath`, which does not follow a pod to another node: a master that is
rescheduled comes back with an empty data directory and a brand new cluster
UUID. With the chart's default of a single master replica there is no peer to
recover the identity from either.

Putting the master's data on a claim avoids that:

```yaml
master:
  data:
    type: "persistentVolumeClaim"
    size: "1Gi"
    storageClass: ""   # empty uses the cluster's default StorageClass
```

Raft state is small, so a modest claim is enough — sizing matters far more for
volume and filer.

The default is left at `hostPath` for backward compatibility:
`volumeClaimTemplates` is immutable on a StatefulSet, so flipping the type on a
release that already exists fails, whether the chart changes the default or you
change it yourself:

```text
StatefulSet.apps "<release>-seaweedfs-master" is invalid: spec: Forbidden:
updates to statefulset spec for fields other than 'replicas', ... are forbidden
```

New installs can set the claim from the start. To move an **existing** release
onto a claim without losing the cluster UUID, use the migration below. The
claim has to be seeded while the master is stopped: a running master rewrites
its Raft state, so copying into a live pod is silently undone by the next
restart.

The steps below are for the chart's default of a single master
(`master.replicas: 1`). With several master replicas, repeat steps 1, 3 and 4
for every ordinal, or migrate one at a time and let the remaining quorum
re-replicate.

Take the names from the cluster rather than assembling them — the release
name, `nameOverride` and `fullnameOverride` all feed the chart's fullname
helper, so `<release>-seaweedfs` is not always right:

```bash
NS=<namespace>; REL=<release>
# scope by instance as well as component: several releases can share a namespace
STS=$(kubectl -n $NS get sts \
        -l app.kubernetes.io/instance=$REL,app.kubernetes.io/component=master \
        -o jsonpath='{.items[0].metadata.name}')
POD=$STS-0
# a StatefulSet names its claims <template>-<statefulset>-<ordinal>, and this
# chart's template is data-<namespace>
PVC=data-$NS-$STS-0

# 1. back up the master data directory
kubectl -n $NS cp $POD:/data ./master-backup

# 2. stop the master, leaving the rest of the release running
kubectl -n $NS delete sts $STS --cascade=orphan
kubectl -n $NS delete pod $POD

# 3. create the claim the new StatefulSet will adopt, and seed it through a
#    pod that actually mounts it
kubectl -n $NS apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $PVC
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: seed
spec:
  containers:
    - name: seed
      image: alpine:3.20
      command: ["sleep", "600"]
      volumeMounts:
        - name: d
          mountPath: /data
  volumes:
    - name: d
      persistentVolumeClaim:
        claimName: $PVC
EOF
kubectl -n $NS wait --for=condition=Ready pod/seed --timeout=120s
kubectl -n $NS cp ./master-backup/m9333 seed:/data/
kubectl -n $NS exec seed -- ls /data/m9333    # conf, log, snapshot, state
kubectl -n $NS delete pod seed

# 4. upgrade; the StatefulSet adopts the claim you created
helm upgrade $REL seaweedfs/seaweedfs -n $NS -f values.yaml
```

Confirm the UUID survived — it must match what the cluster reported before the
migration:

```bash
kubectl -n $NS exec $POD -- curl -s localhost:9333/license/status
```

**Or accept a new cluster UUID** and, if you run the enterprise edition, have
the license re-issued against it.

## current instances config (AIO):

1 instance for each type (master/filer+s3/volume)

You can update the replicas count for each node type in values.yaml,
need to add more nodes with the corresponding labels if applicable.

Most of the configuration are available through values.yaml any pull requests to expand functionality or usability are greatly appreciated. Any pull request must pass [chart-testing](https://github.com/helm/chart-testing).

## S3 configuration

To enable an s3 endpoint for your filer with a default install add the following to your values.yaml:

```yaml
filer:
  s3:
    enabled: true
```

### Enabling Authentication to S3

To enable authentication for S3, you have two options:

- let the helm chart create an admin user as well as a read only user
- provide your own s3 config.json file via an existing Kubernetes Secret

#### Use the default credentials for S3

Example parameters for your values.yaml:

```yaml
filer:
  s3:
    enabled: true
    enableAuth: true
```

#### Provide your own credentials for S3

Example parameters for your values.yaml:

```yaml
filer:
  s3:
    enabled: true
    enableAuth: true
    existingConfigSecret: my-s3-secret
```

Example existing secret with your s3 config to create an admin user and readonly user, both with credentials:

```yaml
---
# Source: seaweedfs/templates/seaweedfs-s3-secret.yaml
apiVersion: v1
kind: Secret
type: Opaque
metadata:
  name: my-s3-secret
  namespace: seaweedfs
  labels:
    app.kubernetes.io/name: seaweedfs
    app.kubernetes.io/component: s3
stringData:
  # this key must be an inline json config file
  seaweedfs_s3_config: '{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":"snu8yoP6QAlY0ne4","secretKey":"PNzBcmeLNEdR0oviwm04NQAicOrDH1Km"}],"actions":["Admin","Read","Write"]},{"name":"anvReadOnly","credentials":[{"accessKey":"SCigFee6c5lbi04A","secretKey":"kgFhbT38R8WUYVtiFQ1OiSVOrYr3NKku"}],"actions":["Read"]}]}'
```

## Admin Component

The admin component provides a modern web-based administration interface for managing SeaweedFS clusters. It includes:

- **Dashboard**: Real-time cluster status and metrics
- **Volume Management**: Monitor volume servers, capacity, and health
- **File Browser**: Browse and manage files in the filer
- **Maintenance Operations**: Trigger maintenance tasks via workers
- **Object Store Management**: Create and manage buckets with web interface

### Enabling Admin

To enable the admin interface, add the following to your values.yaml:

```yaml
admin:
  enabled: true
  port: 23646
  grpcPort: 33646  # For worker connections
  secret:
    adminUser: "admin"
    adminPassword: "your-secure-password"  # Leave empty to disable auth
  
  # Optional: persist admin data
  data:
    type: "persistentVolumeClaim"
    size: "10Gi"
    storageClass: "your-storage-class"
  
  # Optional: enable ingress
  ingress:
    enabled: true
    host: "admin.seaweedfs.local"
    className: "nginx"
```

The admin interface will be available at `http://<admin-service>:23646` (or via ingress). Workers connect to the admin server via gRPC on port `33646`.

### Admin Authentication

If `adminPassword` is set, the admin interface requires authentication:
- Username: Value of `adminUser` (default: `admin`)
- Password: Value of `adminPassword`

If `adminPassword` is empty or not set, the admin interface runs without authentication (not recommended for production).

As an alternative, a kubernetes Secret can be used (`admin.secret.existingSecret`).

### Admin Data Persistence

The admin component can store configuration and maintenance data. You can configure storage in several ways:

- **emptyDir** (default): Data is lost when pod restarts
- **persistentVolumeClaim**: Data persists across pod restarts
- **hostPath**: Data stored on the host filesystem
- **existingClaim**: Use an existing PVC

## Worker Component

Workers are maintenance agents that execute cluster maintenance tasks such as vacuum, volume balancing, and erasure coding. Workers connect to the admin server via gRPC and receive task assignments.

### Enabling Workers

To enable workers, add the following to your values.yaml:

```yaml
worker:
  enabled: true
  replicas: 2  # Scale based on workload
  jobType: "vacuum,volume_balance,erasure_coding"  # Job types this worker can handle
  maxDetect: 1  # Maximum concurrent detection requests
  maxExecute: 4  # Maximum concurrent execution jobs per worker
  
  # Working directory for task execution
  # Default: "/tmp/seaweedfs-worker"
  # Note: /tmp is ephemeral - use persistent storage (hostPath/existingClaim) for long-running tasks
  workingDir: "/tmp/seaweedfs-worker"
  
  # Optional: configure admin server address
  # If not specified, auto-discovers from admin service in the same namespace by looking for
  # a service named "<release-name>-admin" (e.g., "seaweedfs-admin").
  # Auto-discovery only works if the admin is in the same namespace and same Helm release.
  # For cross-namespace or separate release scenarios, explicitly set this value.
  # Example: If main SeaweedFS is deployed in "production" namespace:
  #   adminServer: "seaweedfs-admin.production.svc:33646"
  adminServer: ""
  
  # Workers need storage for task execution
  # Note: Workers use a Deployment, which does not support `volumeClaimTemplates` 
  # for dynamic PVC creation per pod. To use persistent storage, you must 
  # pre-provision a PersistentVolumeClaim and use `type: "existingClaim"`.
  data:
    type: "emptyDir"  # Options: "emptyDir", "hostPath", or "existingClaim"
    hostPathPrefix: /storage  # For hostPath
    # claimName: "worker-pvc"  # For existingClaim with pre-provisioned PVC
  
  # Resource limits for worker pods
  resources:
    requests:
      cpu: "500m"
      memory: "512Mi"
    limits:
      cpu: "2"
      memory: "2Gi"
```

### Worker Job Types

Workers can be configured with different job types:
- **vacuum**: Reclaim deleted file space
- **volume_balance**: Balance volumes across volume servers
- **erasure_coding**: Handle erasure coding operations

You can configure workers with all job types or create specialized worker pools with specific job types.

### Worker Deployment Strategy

For production deployments, consider:

1. **Multiple Workers**: Deploy 2+ worker replicas for high availability
2. **Resource Allocation**: Workers need sufficient CPU/memory for maintenance tasks
3. **Storage**: Workers need temporary storage for vacuum and balance operations (size depends on volume size)
4. **Specialized Workers**: Create separate worker deployments for different job types if needed

Example specialized worker configuration:

For specialized worker pools, deploy separate Helm releases with different job types:

**values-worker-vacuum.yaml** (for vacuum operations):
```yaml
# Disable all other components, enable only workers
master:
  enabled: false
volume:
  enabled: false
filer:
  enabled: false
s3:
  enabled: false
admin:
  enabled: false

worker:
  enabled: true
  replicas: 2
  jobType: "vacuum"
  maxExecute: 2
  # REQUIRED: Point to the admin service of your main SeaweedFS release
  # Replace <namespace> with the namespace where your main seaweedfs is deployed
  # Example: If deploying in namespace "production":
  #   adminServer: "seaweedfs-admin.production.svc:33646"
  adminServer: "seaweedfs-admin.<namespace>.svc:33646"
```

**values-worker-balance.yaml** (for balance operations):
```yaml
# Disable all other components, enable only workers
master:
  enabled: false
volume:
  enabled: false
filer:
  enabled: false
s3:
  enabled: false
admin:
  enabled: false

worker:
  enabled: true
  replicas: 1
  jobType: "volume_balance"
  maxExecute: 1
  # REQUIRED: Point to the admin service of your main SeaweedFS release
  # Replace <namespace> with the namespace where your main seaweedfs is deployed
  # Example: If deploying in namespace "production":
  #   adminServer: "seaweedfs-admin.production.svc:33646"
  adminServer: "seaweedfs-admin.<namespace>.svc:33646"
```

Deploy the specialized workers as separate releases:
### Specialized Worker Deployment
```bash
# Deploy vacuum workers
helm install seaweedfs-worker-vacuum seaweedfs/seaweedfs -f values-worker-vacuum.yaml

# Deploy balance workers
helm install seaweedfs-worker-balance seaweedfs/seaweedfs -f values-worker-balance.yaml
```

## Network Policies

In a namespace with a default-deny policy the install hangs: the components cannot resolve each other, and the post-install bucket hook waits on the master and filer until it gives up. `networkPolicy.enabled` renders one `NetworkPolicy` per component, selecting its pods by the standard `app.kubernetes.io/{name,instance,component}` labels and admitting traffic from the other pods of the release on the ports that component listens on.

```bash
helm install seaweedfs seaweedfs/seaweedfs --set networkPolicy.enabled=true
```

That alone leaves outbound traffic untouched, which is enough when the namespace's default-deny only restricts ingress. If it lists `Egress` in its `policyTypes` too - the usual baseline - the components still cannot resolve DNS, and you need the second opt-in below as well.

Egress is separate because the chart knows where its own components live but not where your filer store, notification sink or remote tier does, and because in a namespace with no default-deny at all, adding egress rules would narrow the components from "may reach anything" to "may reach these peers":

```yaml
networkPolicy:
  enabled: true
  egress:
    enabled: true
    kubeApiServer:
      # the endpoint behind the kubernetes service, not its ClusterIP
      cidrs: ["172.18.0.2/32"]
    extraEgress:
      - to:
          - podSelector:
              matchLabels:
                app.kubernetes.io/name: postgresql
        ports:
          - protocol: TCP
            port: 5432
```

`kubeApiServer.cidrs` is only demanded when something in the release actually needs the API server, which is the COSI sidecar and, on an upgrade that grows a volume PVC, the resize hook. No seaweedfs component itself speaks to it.

Anything reaching the release from outside - an ingress controller, a Prometheus in another namespace - goes into `networkPolicy.extraIngress`, or into `networkPolicy.components.<component>.extraIngress` for a single component. See the `networkPolicy` block in `values.yaml` for the full set.

Two things worth knowing before you turn this on:

- **Monitoring stops.** The metrics ports are admitted from release pods like every other port, so with `global.seaweedfs.monitoring.enabled` the ServiceMonitors keep scraping targets a Prometheus in another namespace can no longer reach. Nothing reports it; add the scraper's namespace to `extraIngress`.
- **The resize hook's policy is a Helm hook.** Its Job runs before the release manifest is applied, so the policy has to be a `pre-install` hook too. Helm does not garbage-collect hook resources, so on an upgrade that grows a volume PVC the policy is created and then left behind on uninstall - delete `<release>-seaweedfs-volume-resize-hook` by hand if it bothers you.

The DNS selectors default to CoreDNS as kubeadm, kind and the managed offerings from AWS, Google and Azure install it. On OpenShift, override `egress.dnsNamespaceSelector` and `egress.dnsPodSelector` to match `openshift-dns`; see the comment in `values.yaml`.

## OpenShift Support

SeaweedFS can be deployed on OpenShift or any cluster enforcing the Kubernetes "restricted" Pod Security Standard. By default, OpenShift blocks containers that run as root or use `hostPath` volumes.

To deploy on OpenShift, use the provided `openshift-values.yaml` which overrides the default configuration to:
1. Use `PersistentVolumeClaims` instead of `hostPath`.
2. Enable `runAsNonRoot` and omit hardcoded UIDs to allow OpenShift to assign valid UIDs automatically.
3. Apply appropriate `seccompProfile` and drop capabilities.

Usage:
```bash
helm install seaweedfs seaweedfs/seaweedfs \
  -n seaweedfs --create-namespace \
  -f openshift-values.yaml
```

## Enterprise

For enterprise users, please visit [seaweedfs.com](https://seaweedfs.com) for the SeaweedFS Enterprise Edition, 
which has advanced features, including data recovery, self-healing storage, customizable erasure coding, EC vacuum and repair, etc.

To run it, set the image and point the chart at a Secret holding the license
file:

```bash
kubectl create secret generic seaweedfs-license -n <namespace> \
  --from-file=seaweed-license.json=/path/to/seaweed-license.json
```

```yaml
global:
  seaweedfs:
    image:
      name: chrislusf/seaweedfs-enterprise
    license:
      existingSecret: seaweedfs-license
      # secretKey: seaweed-license.json     # key within the Secret
      # mountPath: /etc/seaweedfs/license   # directory it is mounted at
```

Set the image globally rather than per component: a per-component
`imageOverride` wins, and a cluster that mixes editions comes up looking
healthy with enterprise features quietly off.

Only the master reads the license, so the Secret is mounted read-only there
and on all-in-one (which runs `weed server -master`). It is mounted as a
directory, not a `subPath`, so a renewed Secret reaches the running master —
which re-reads the file periodically — without a restart.

The license is tied to the cluster UUID kept in the master's Raft state, so put
`master.data` on a claim — a master that restarts onto an empty data directory
generates a new UUID and the license stops matching. See
[Master data](#master-data-hostpath-vs-a-claim). Check the binding with
`kubectl exec <master-pod> -- curl -s localhost:9333/license/status`
(`cluster_uuid` must equal `license_uuid`).
