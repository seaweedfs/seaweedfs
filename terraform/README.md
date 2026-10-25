# SeaweedFS on Terraform

> **Experimental.** This Terraform support is an early scaffold under active
> development. Interfaces (variables, outputs, module layout) may change without
> notice, and not every tier is implemented yet. Not recommended for production
> without your own review and testing.

Self-contained Terraform/OpenTofu modules to deploy SeaweedFS on cloud VMs
running the `weed` binary directly under systemd. No Helm and no Kubernetes
required.

What works today is verified end-to-end against a real `weed` cluster (see
"Test it locally" below).

## Layout

```text
terraform/
  modules/
    core/        cloud-agnostic renderer (ZERO cloud resources): turns an
                 address map + config into per-node weed argv, systemd units,
                 config files, disk-mount + secret-fetch scripts, and cloud-init.
                 Both the cloud wrappers and the local harnesses consume `nodes`.
    security/    cloud-agnostic CA + per-component mTLS certs (distinct CNs) +
                 JWT signing keys (tls/random providers). Emits a `core_security`
                 object ready for the core, plus PEMs for secret-store delivery.
    aws/         thin AWS wrapper: reserves stable ENIs (fixed private IPs)
                 first, feeds them to core, creates instances + protected EBS
                 data disks + SG; with enable_security it generates certs/JWT,
                 stores them in SSM SecureString, grants an instance role, and
                 renders a boot fetch-secrets.sh. Keyed for_each throughout.
  examples/
    aws-ha-distributed/   3 masters + 3 volumes (1/AZ) + 2 filers + 1 S3,
                          secure-by-default (mTLS via SSM-delivered certs)
    aws-all-in-one/       single `weed server` instance via core directly
  test/
    local/         render a cluster with core and run it as real weed processes
                   on 127.0.0.1 (no cloud, no docker), then assert it works
    local-secure/  generate certs/JWT with the security module, run a real
                   mTLS cluster, and assert it forms + enforces JWT auth
```

## Design in one paragraph

The chart is the structural reference, not a dependency. A cloud-agnostic
**core** renders everything portable; thin per-cloud wrappers provision infra.
Addressing is an **input** to the core (wrapper reserves static IPs first), so
the wrapper -> core dependency is one-way with no apply-time cycle. Stateful
tiers (master/volume/filer) are keyed `for_each` maps, never `count`, so a
middle node can be replaced without reindexing its peers or reattaching the
wrong disk. Flag names are verified against the real `weed` binary
(notably: volume uses `-mserver` for the master list; gRPC is `-port.grpc`,
auto = http+10000; `minFreeSpacePercent` is a string).

## Test it locally

Requires `tofu` (or `terraform`), `jq`, `curl`, and a `weed` binary. Renders a
3-master + volume + filer + S3 cluster from the core module and runs it as real
processes, asserting quorum, volume registration, and filer/S3 round-trips:

```bash
cd terraform/test/local
WEED=/path/to/weed ./run_local_cluster.sh
# => 7 passed, 0 failed
```

The harness uses a high port range (29333/28080/28888/28333) so it does not
collide with a SeaweedFS cluster already running on the machine, and aborts if a
required port is taken. `KEEP=1 ./run_local_cluster.sh` leaves the cluster up.

### mTLS end-to-end

```bash
cd terraform/test/local-secure
WEED=/path/to/weed ./run_local_secure.sh
# => generates a CA + component certs + JWT, renders security.toml, runs a real
#    mTLS cluster, asserts master/volume/filer form over mutual TLS and that the
#    filer enforces JWT signing (unsigned writes get 401). 5 passed.
```

## Plan-level tests (no cloud)

```bash
cd terraform/modules/core && tofu test
# => 11 passed: peers list, -mserver vs -master, metrics gating,
#    security.toml conditions, all-in-one inheritance, ...
```

## Validate the cloud wrappers

```bash
cd terraform/examples/aws-ha-distributed && tofu init && tofu validate
```

`apply` needs AWS credentials, a VPC, subnets, and an AMI with `weed` installed
(bake with Packer, or install at boot).

## Status

Implemented and verified:
- Tiers: master / volume / filer / s3 / all-in-one rendered by the core.
- Disk mount: cloud-init runs a `mount-disks.sh` that auto-discovers (or takes
  explicit candidate devices), `blkid`-guards `mkfs`, mounts, and persists to
  `/etc/fstab` by UUID. The AWS wrapper wires the protected EBS disk to `/data`.
- mTLS + JWT: the `security` module generates the CA + per-component certs
  (distinct CNs) + JWT keys; the AWS wrapper stores them in SSM SecureString,
  grants an instance role, and the core renders a boot fetch script so secrets
  stay OUT of user_data. Proven end-to-end by `test/local-secure`.

Not yet implemented:
- sftp / admin / worker tiers (core does not render them yet).
- GCP/Azure wrappers, the data-plane provider, and the K8s-native module.
- CA in Vault PKI (currently TF-generated, so the CA key lives in state). KMS
  decrypt in the IAM policy is scoped by
  `ViaService` but uses `Resource="*"`; tighten to the SSM CMK in production.

## Gotchas

- This `weed` build starts an Iceberg REST catalog on `:8181` by default; set
  `s3.iceberg_port = 0` to disable, or a port to relocate it.
- Disk auto-discovery assumes a single attached data disk per node. For multiple
  data disks, pass explicit `devices` candidates in `disk_mounts`.

## Security note

`tofu output`/state can contain secrets. Generate secrets outside Terraform
(cloud secret manager / Vault) and fetch them at boot; never commit secret
material.
