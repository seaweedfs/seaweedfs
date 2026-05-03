# V3 Phase 15 G9G-3 - Cluster Spec YAML Facade Mini-Plan

Date: 2026-05-03
Status: implemented at `seaweed_block@4fc0842`; QA verification pending
Branch target: `p15-g9g3/cluster-spec-yaml`
Scope: declarative bootstrap YAML that feeds existing lifecycle/product-loop stores

## 0. Why This Gate Exists

G9G closed the first product-loop assignment path using `--lifecycle-placement-seed`.
That seed file is a good QA bridge but not a product-shaped operator surface.

G9G-3 introduces a small declarative facade:

```powershell
blockmaster --cluster-spec m01.yaml --authority-store ... --lifecycle-store ...
```

The YAML is converted into existing lifecycle facts; it does not bypass the
G9D/F/G9F-2/G9G chain.

## 1.A Bindings

1. **Facade only**: cluster spec imports lifecycle facts; it must not call publisher directly.
2. **No new truth**: YAML fields become desired volume, node inventory, and placement intent only.
3. **Existing pipeline**: assignment still requires product-loop verification and publisher minting.
4. **No proto/API change**: first slice is startup bootstrap only.
5. **Temporary simplicity**: schema can cover existing-replica placement first; blank-pool allocation remains follow-up.

## 2. Minimal YAML Shape

```yaml
volumes:
  - id: v1
    size_bytes: 1048576
    replication_factor: 1

nodes:
  - id: s2
    data_addr: 127.0.0.1:9202
    ctrl_addr: 127.0.0.1:9102
    replicas:
      - volume_id: v1
        replica_id: r2
        size_bytes: 1048576

placements:
  - volume_id: v1
    desired_rf: 1
    slots:
      - server_id: s2
        replica_id: r2
        source: existing_replica
```

## 3. TDD Plan

1. `TestClusterSpec_ImportsDesiredVolumeNodeAndPlacement`
2. `TestParseFlags_ClusterSpecOptional`
3. `TestG9G_L2ClusterSpecPublishesAssignmentToBlockvolume`

## 4. Close Criteria

1. YAML imports into lifecycle stores.
2. Existing G9G product loop consumes imported placement.
3. Real subprocess L2 reaches blockvolume assignment-backed `Healthy=true`.
4. No authority publisher call is added to the import path.

## 6. Implementation Snapshot

Close candidate: `seaweed_block@4fc0842`.

Implemented:

- `cmd/blockmaster --cluster-spec <yaml>`;
- YAML imports accepted topology and placement intent;
- existing G9G product loop consumes the imported placement;
- focused L2 now uses `--cluster-spec` instead of `--lifecycle-placement-seed`.

QA commands:

```powershell
go test ./cmd/blockvolume -run TestG9G_L2ProductLoopPublishesAssignmentToBlockvolume -count=1
go test ./core/lifecycle ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

## 5. Non-Claims

- Not a full product API.
- Not CSI.
- Not dynamic reconfiguration.
- No blank-pool replica-id allocator.
- No M01 hardware evidence.
