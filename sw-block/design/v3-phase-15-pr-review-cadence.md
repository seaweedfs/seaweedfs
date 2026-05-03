# V3 Phase 15 PR / Review Cadence

Date: 2026-05-03
Status: active process note

## Rule

Use one PR per coherent gate or milestone, not one PR per small commit.

## Why

P15 now has many small TDD slices. Opening every slice as a PR causes review
noise and burns CodeRabbit / reviewer attention on intermediate states that are
not meaningful product checkpoints.

## Cadence

1. Work on a gate branch, e.g. `p15-g9g/blockmaster-product-loop`.
2. Land small commits freely inside that branch:
   - mini-plan / docs;
   - red test;
   - implementation;
   - focused verification;
   - close snapshot.
3. Ask QA to verify the branch at a pinned commit.
4. Open or update PR only when the branch has a coherent review target.
5. Merge to `phase-15`.
6. Draft PR `phase-15 -> main` remains the high-level integration visibility surface.

## Exceptions

Open an earlier PR if:

- the change crosses an authority/protocol boundary and needs external review;
- the branch is getting too large to review coherently;
- CodeRabbit feedback is specifically needed before more work continues;
- another developer needs the branch integrated into `phase-15`.

## Current Application

G9G used this cadence:

- `p15-g9g/blockmaster-product-loop` accumulated product-loop slices through QA sign.
- It merged once into `phase-15@2b90018`.
- `p15-g9g3/cluster-spec-yaml` was a distinct usability follow-up and merged once into `phase-15@2d13b02`.

This is the preferred pattern for the next P15 gates.
