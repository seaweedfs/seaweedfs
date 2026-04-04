# V2 Legacy Runtime Exit Criteria

Date: 2026-04-04
Status: active

## Purpose

This note defines when legacy runtime-owner paths may be downgraded from
required compatibility coverage to removable implementation history.

Current legacy examples:

1. `legacy P4` live-path proofs
2. no-core startup paths
3. `HandleAssignmentResult()`-driven recovery startup kept for compatibility

## Current Position

For the current phase, legacy paths must remain:

1. as compatibility guards
2. as regression protection for no-core behavior
3. but NOT as semantic authority proof for the core-present path

## Exit Criteria

A legacy runtime-owner path may be downgraded or removed only when all of the
following are true.

### 1. V2-native proof replacement exists

There must be core-present proofs covering the same behavior category:

1. assignment entry ownership
2. task startup ownership
3. execution ownership
4. observation return ownership
5. outward surface consistency

### 2. Compatibility mode is no longer required operationally

At least one of these must be true:

1. production startup always wires `v2Core`
2. no-core path is explicitly declared unsupported
3. no remaining product surface depends on no-core runtime startup

### 3. The legacy path is no longer the only guard for a runtime mechanic

Examples:

1. serialized replacement/drain behavior
2. shutdown drain behavior
3. live plan-to-execute behavior

These must have equivalent core-present coverage before legacy deletion.

### 4. No semantic truth still depends on legacy behavior

Specifically, removing the legacy path must not change:

1. identity meaning
2. recovery classification
3. publication meaning
4. durable-boundary meaning

If removal changes any of those, the legacy path was still hiding semantic
authority and cannot be retired yet.

## Downgrade Stages

Legacy paths should retire in stages:

### Stage 1: authority downgrade

1. keep tests
2. explicitly classify them as compatibility-only

### Stage 2: runtime fallback downgrade

1. keep fallback code only where product startup still needs it
2. stop expanding proof claims from those paths

### Stage 3: deletion candidate

1. delete tests or move them to legacy-only coverage
2. remove runtime fallback code only after the new path is already the sole
   supported owner

## Current Judgment

As of the current separation work:

1. `legacy P4` stays
2. it is already downgraded to compatibility guard
3. it is not yet removable because:
   - no-core behavior still exists
   - full runtime-loop closure is not yet complete
   - not every old ownership proof has a complete core-present replacement
