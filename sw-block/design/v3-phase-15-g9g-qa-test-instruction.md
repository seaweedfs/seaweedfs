# V3 Phase 15 G9G QA Test Instruction

Date: 2026-05-03
Status: close-candidate on `p15-g9g/blockmaster-product-loop@7ed9ab2`
Scope: blockmaster product loop publishes verified placement into the authority publisher path

## Headline

At `p15-g9g/blockmaster-product-loop@7ed9ab2`, G9G proves a real blockmaster
can import placement intent through a daemon entry point, run the lifecycle
product loop, publish through `authority.Publisher`, and deliver assignment to
a real blockvolume subscriber.

## Environment

- Repo: `seaweed_block`
- Branch: `p15-g9g/blockmaster-product-loop`
- Minimum commit: `7ed9ab2`
- Fidelity: subprocess L2 with real `cmd/blockmaster` and real `cmd/blockvolume`
- Non-claim: not m01/M02 hardware and not final product API

## Full Regression Command

```powershell
go test ./core/lifecycle ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Expected result: all five packages pass. `cmd/blockvolume` is expected to take
about two minutes because it includes G8 and G9 subprocess tests.

## Focused G9G Command

```powershell
go test ./cmd/blockvolume -run TestG9G_L2ProductLoopPublishesAssignmentToBlockvolume -count=1
```

Expected result: pass in roughly 10-15 seconds on the local Windows harness.

## Scenario Checklist

1. Blockmaster accepts a product-loop seed entry.
   Backing test: `cmd/blockmaster/main_test.go::TestParseFlags_LifecyclePlacementSeedOptional`

2. Product-loop component tick publishes verified existing replica placement.
   Backing test: `core/host/master/product_loop_test.go::TestG9G_ProductLoopPublishesVerifiedExistingReplica`

3. Unverified placement does not publish authority.
   Backing test: `core/host/master/product_loop_test.go::TestG9G_ProductLoopDoesNotPublishUnverifiedPlacement`

4. Repeated product-loop tick does not churn authority.
   Backing test: `core/host/master/product_loop_test.go::TestG9G_ProductLoopIsIdempotentForSameAuthorityLine`

5. Publisher subscription receives product-loop assignment.
   Backing test: `core/host/master/product_loop_test.go::TestG9G_BlockvolumeSubscriptionReceivesProductLoopAssignment`

6. Real subprocess path delivers assignment to blockvolume.
   Backing test: `cmd/blockvolume/g9g_l2_product_loop_test.go::TestG9G_L2ProductLoopPublishesAssignmentToBlockvolume`

## Non-Claims

- `--lifecycle-placement-seed` is a temporary QA/M01 bridge, not the final product API.
- No YAML cluster spec yet.
- No external create/delete/attach/detach API yet.
- No blank-pool replica-id allocator.
- No CSI integration.
- No m01/M02 hardware evidence.
- Assignment publication still does not imply frontend readiness in the abstract; this L2 checks the existing blockvolume status projection after assignment delivery.

## Closure Sentence

G9G closes the first live product-loop assignment path:
`seeded placement intent -> blockmaster product loop -> publisher -> blockvolume assignment stream`.
