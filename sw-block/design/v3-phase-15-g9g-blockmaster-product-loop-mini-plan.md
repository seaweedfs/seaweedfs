# V3 Phase 15 G9G - Blockmaster Product Loop To Publisher Mini-Plan

Date: 2026-05-03
Status: component slice implemented at `seaweed_block@afac861`; subprocess L2 implemented at `seaweed_block@bdd56c7`; seed-file entry implemented at `seaweed_block@7ed9ab2`; QA verification pending
Branch target: `p15-g9g/blockmaster-product-loop`
Scope: first live blockmaster loop that turns verified placement into publisher input

## 0. Why This Gate Exists

G9D/F proved the fact layer:

```text
DesiredVolume + NodeInventory + PlacementIntent + Observation -> VerifiedPlacement
```

G9F-2 proved the ask bridge:

```text
VerifiedPlacement -> authority.AssignmentAsk
```

G9G is the first product-loop slice:

```text
blockmaster lifecycle loop -> G9F-2 bridge -> authority.Publisher -> AssignmentFact
```

This is the first point where product lifecycle state can cause a real assignment
to be published. That is useful for M01, but it is also the authority boundary.

## 1.A Architect Bindings

Bindings used by the first product-loop slice:

1. **Publisher-only minting**: G9G may feed `authority.AssignmentAsk` into the existing publisher path; it must not construct `AssignmentInfo` or `AssignmentFact`.
2. **Verified-only input**: only `VerifiedPlacement{Verified:true}` can produce a live ask.
3. **Negative inputs**: placement-only, observation-only, and unverified placement must not publish.
4. **Loop idempotency**: repeated product-loop ticks for the same verified placement must not create duplicate authority churn.
5. **No proto change**: first slice uses existing in-process host/master seams and existing assignment subscription protocol.
6. **No frontend-ready shortcut**: receiving/publishing an assignment is not the same as frontend readiness.
7. **Stop rule**: if create-volume API, CSI, replica allocation for blank pools, or control-proto changes are needed, split a follow-up gate.

## 2. Algorithm Sketch

For one product-loop tick:

1. Load lifecycle stores.
2. Reconcile desired volumes + node inventory into placement intents.
3. Verify placements against observation.
4. Convert verified placements to assignment asks via G9F-2 bridge.
5. Append asks into the master publisher directive seam.
6. Publisher mints epoch/endpoint-version and fan-outs assignment facts.

The loop must skip asks that already match the publisher's current authority
line. First slice can handle existing-replica placement only; blank-pool
replica-id allocation remains out of scope.

## 3. TDD Plan

Red tests first:

1. `TestG9G_ProductLoopPublishesVerifiedExistingReplica`
   - lifecycle placement + fresh observation;
   - product-loop tick emits one ask;
   - real publisher mints one authority line.

2. `TestG9G_ProductLoopDoesNotPublishUnverifiedPlacement`
   - placement exists but observation missing/stale;
   - publisher line remains absent.

3. `TestG9G_ProductLoopIsIdempotentForSameAuthorityLine`
   - two ticks against same verified placement;
   - publisher line remains stable; no epoch churn.

4. `TestG9G_BlockvolumeSubscriptionReceivesProductLoopAssignment`
   - subscriber listens through existing assignment stream/seam;
   - product-loop tick publishes assignment;
   - subscriber receives the minted assignment fact.

5. `TestG9G_NoDirectAssignmentInfoConstructionOutsideAllowlist`
   - existing structural guard remains green.

## 4. Close Criteria

G9G first slice closes when:

1. tests in §3 pass;
2. no proto/control API change;
3. no `AssignmentInfo` construction outside authority/allowed decoder seams;
4. blockmaster host has a callable product-loop tick;
5. docs state that assignment publication still does not imply frontend readiness.

## 5. Non-Claims

- No external create-volume API.
- No CSI integration.
- No blank-pool replica id allocator.
- No M01 hardware smoke.
- No quorum/full-ACK policy.
- No frontend-ready claim.

## 6. Next After G9G

If G9G closes, the next M01-oriented slice is a subprocess L2:

```text
real blockmaster + real blockvolume subscription + product-loop tick -> assignment delivered
```

This subprocess L2 landed at `seaweed_block@bdd56c7` and was upgraded at
`seaweed_block@7ed9ab2` to use a blockmaster seed-file entry instead of
pre-writing lifecycle store internals:

- real `cmd/blockmaster` with `--lifecycle-store`;
- `--lifecycle-placement-seed <json>` imports placement intent through the daemon entry point;
- real `cmd/blockvolume` subscribing to assignment stream;
- only r2 observed, so legacy topology controller cannot satisfy RF=2 by itself;
- lifecycle product loop publishes the verified existing-r2 placement;
- r2 reaches assignment-backed `Healthy=true`.

Only after that should we wire external API/CLI verbs or CSI create/publish.
