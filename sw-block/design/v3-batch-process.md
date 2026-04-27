# V3 Batch Process

**Date**: 2026-04-26
**Status**: ACTIVE — applies to all batches from G5-5 onward
**Supersedes**: ad-hoc per-batch governance (kickoff + mini-plan + G-1 + closure report + forward-carry checklist + QA scenario catalogue, 5+ artifacts)

---

## §1 The flow

```
plan → (G-1 if V2 PORT) → code → test → close
```

One mini-plan doc per batch. One PR. One close.

---

## §2 The single mini-plan doc

`v3-phase-15-<batch-id>-mini-plan.md` — the only artifact per batch. Sections in order:

| § | Content | Required when |
|---|---|---|
| §1 Scope | What this batch delivers; what it does NOT (explicit non-claims) | Always |
| §2 Acceptance criteria | Numbered list, each verifier-named (test or evidence), SINGLE source of truth | Always |
| §3 Invariants | INV IDs to inscribe at close + their test pointers; ledger updated by PR (not after) | Always |
| §4 G-1 V2 read | Inline: PORT items + V3-NATIVE items + hidden invariant audit | **Only if V2 muscle PORT batch** |
| §5 Forward-carry consumed | What this batch inherits from prior batch's §close | Always (even if "none") |
| §6 Risks + mitigations | Short table | Always |
| §7 Sign table | architect ratify §1-§6 once at start; architect single-sign §close at close | Always |
| §close | Appended at batch close: deltas vs §1-§6 + evidence pointers + forward-carries to next batch | Appended at close (not separate doc) |

**Total per batch: 1 doc, 1 commit lifecycle (init → §close append → architect sign).** No separate closure report, no separate G-1 doc, no separate forward-carry checklist, no separate QA scenario catalogue.

---

## §3 What earned its keep — KEEP

These survived T4 with track record. Don't drop:

| Discipline | When it applies | What it catches |
|---|---|---|
| **G-1 V2 PORT read** (inline §4) | V2 muscle ports only | Hidden invariants, architectural pins, placement decisions pre-code (5 saves on T4) |
| **Mini-plan acceptance criteria** (§2) | Always | Ambiguous closes; PR review checklist |
| **Invariant ledger discipline** (§3 + `v3-invariant-ledger.md`) | Always | "Claim without test = wish" rule |
| **m01 -race verification** | Always for any concurrency-touching code | Windows-blind concurrency bugs (caught 2 engine bugs at T4d-4 part C) |
| **Architect single-sign at close** | Always | Scope drift, stale references, silent narrowing |

---

## §4 What was overhead — DROP

| Practice | Reason dropped |
|---|---|
| Separate kickoff PROPOSAL doc | Mini-plan §1-§6 ratification is the same thing |
| Separate G-1 V2 read doc | Inline §4 of mini-plan |
| Separate closure report doc | §close section of mini-plan |
| Separate forward-carry checklist | §5 of next-batch mini-plan |
| Separate QA scenario catalogue | Author tests directly when ready (catalogue went stale 100% of the time) |
| Multi-version doc churn (v0.1 → v0.5) | One ratification + one close-sign per batch; mid-flight revisions only if SCOPE changes |
| Cross-doc invariant restatement (catalogue + checklist + ledger + mini-plan + closure) | Ledger is sole source of truth; other docs reference by INV ID |
| G-N sub-batch invention (G5-1..G5-6 style) | Architect picks T-track OR G-N batches at kickoff, not both for same gate |

---

## §5 Sign cycles — compressed

**Was:** kickoff ratify → mini-plan ratify → G-1 ratify → code → close ratify (4-5 architect signs per batch)

**Now:** mini-plan §1-§6 ratify → code → §close architect sign (2 signs per batch)

Mid-flight revisions ONLY when scope changes (architect re-ratifies the changed §). Doc-hygiene fixes don't need ratification — apply, commit, move on.

---

## §6 Decision rules

### §6.1 G-1 yes/no

| Batch type | G-1 needed? |
|---|---|
| V2 muscle port (`weed/storage/blockvol/*` source) | **YES** — inline §4 |
| V3-native (no V2 source; component framework or new design) | NO |
| Mixed (some PORT items + some V3-native) | YES for the PORT items only |

### §6.2 T-track vs G-N batch naming

| Implementation weight | Naming |
|---|---|
| Substantial new code (>500 LOC) | T-track (T5, T6, ...) |
| Mostly verification + missing pieces | G-N batches (G_x_-1, G_x_-2, ...) |
| Architect picks at kickoff | Don't mix both for same gate |

### §6.3 When to skip the mini-plan entirely

Hotfix-class single-commit changes (1-line + regression test, like `f6084ee` BlockStore walHead) don't need a mini-plan. Architect-approved hotfix PR + ledger update if invariant-affecting.

---

## §7 Doc lifecycle rules

1. **One mini-plan doc per batch.** No separate kickoff/closure/checklist/catalogue artifacts.
2. **Inscribe invariants in `v3-invariant-ledger.md` ONLY** at close. Other docs reference by INV ID.
3. **Update `v3-dev-roadmap.md` at every gate-close** (not batch-close — gate-close).
4. **Doc edits that don't change scope don't need architect ratification** — fix typos, fix stale refs, commit, move on.
5. **Stale doc refs at close-sign time = QA's fault.** Run `grep -r <stale-ref>` before submitting closure for sign.

---

## §8 What to keep current as control docs

These are first-order references; keep updated as batches close:

| Doc | Owner | Update trigger |
|---|---|---|
| [`v3-dev-roadmap.md`](./v3-dev-roadmap.md) | QA | every gate-close |
| [`v3-architecture.md`](./v3-architecture.md) | architect | component/responsibility/flow changes |
| [`v3-phase-15-mvp-scope-gates.md`](./v3-phase-15-mvp-scope-gates.md) | architect | scope changes (e.g. G9A added 2026-04-26) |
| [`v3-invariant-ledger.md`](./v3-invariant-ledger.md) | sw + QA | every batch-close (PR-atomic) |
| [`v3-block-behavior-contract-index.md`](./v3-block-behavior-contract-index.md) | architect | new behavior contract ratifications |
| [`v3-product-placement-authority-rationale.md`](./v3-product-placement-authority-rationale.md) | architect | placement architecture changes |
| [`v2-v3-contract-bridge-catalogue.md`](./v2-v3-contract-bridge-catalogue.md) | sw + QA | V2→V3 entity bridge updates |

Everything else is per-batch (mini-plan) or methodology (`v3-phase-development-model.md`, this doc).

---

## §9 First trial: G5-5

This process gets its first trial at G5-5 (m01 hardware first-light). Concretely:

- ONE doc: `v3-phase-15-g5-5-mini-plan.md`
- §close appended at batch close, not separate report
- Forward-carries from G5-4 (criteria 3+4) consumed in §5
- §close pushes any G5-5 carries forward to G5-6
- Architect: ratify §1-§6 once + sign §close at close
- No separate kickoff, no separate closure report, no separate checklist

If G5-5 closes cleanly under this process, codify as default for G5-6 + G6 + onward.

---

## §10 Process change protocol

This doc evolves like any other:
- New batch tries something different → propose change here
- 3+ batches use the new pattern successfully → codify
- Process change requires architect sign (same §8C.2 rule applies)

Do NOT auto-port T4 governance template forward. Each batch asks "does this step earn its keep" at kickoff.

---

## §11 Honesty principle

Documentation overhead that doesn't catch bugs is ceremony. Documentation that catches bugs is discipline.

Drop ceremony. Keep discipline.

---

## §12 Architect review checklist

Architect review is not plan authorship. It is the system-level check that prevents drift.

Every mini-plan ratification and §close sign must answer:

| Check | Question |
|---|---|
| Scope truth | What is done, what is explicitly not done, and what product risk remains? |
| V2 / new-build decision | Is this a full V2 port, partial V2 port, reference-only read, explicit reject, or V3-native design? Does it need a new gate? |
| Engine / adapter impact | Does this change authority, session fate, recovery, durability, projection, or truth ownership? If yes, where is the invariant and adversarial test? |
| Product usability level | What can an operator or user actually do end-to-end after this batch? Which test proves it? Which next gate makes it more usable? |

Close signs should include a short statement of:

```text
Done:
Not done:
Product level reached:
Next gate that makes it usable:
```

---

## §13 Failure modes this process must prevent

These are the observed sources of waste. Every batch should actively check them.

| Failure mode | Required countermeasure |
|---|---|
| V2 porting drift | Top-down mapping: V2 behavior → V3 product contract → truth owner → accepted/rejected semantics → invariant/test |
| Engine/adapter change without reason | Require reason, truth-domain impact, bad state prevented, and proof test |
| Function close means happy path only | Acceptance must include stale input, reordering/overlap where relevant, partial progress, retry/restart, and explicit non-claims |
| Product unusable despite green tests | Mini-plan must name the end-to-end operator/user path and whether it is real, simulated, manual, or deferred |
| Missing architecture component | If a batch discovers a product component late (for example placement or binary wiring), update `v3-architecture.md` or the gate before continuing |

---

## §14 Who owns what step

Based on what actually worked across T4 + G5:

| Step | Owner | Why |
|---|---|---|
| **Gate scope** (which gate, what it promises, dependencies) | architect | Product requirements + V3 architectural discipline |
| **Batch sketch** (mini-plan §1-§6) | **sw** | Knows the code + implementation feasibility + framework state; can commit to what's deliverable |
| **G-1 V2 read** (when V2 PORT) | sw | Owns the V3 target where V2 muscle lands |
| **Mini-plan ratification** | architect (signs §1-§6) + QA (reviews) | Architect: scope drift / missing bindings / V2 lessons. QA: invariant + acceptance discipline. |
| **Code + unit tests** | sw | — |
| **Component scenarios + m01 verification** | QA | Independent perspective; framework + hardware coverage |
| **Ledger inscription** (PR-atomic) | sw | Lands with code in same PR |
| **§close append** | sw drafts; QA verifies evidence pointers | Sw: facts. QA: acceptance discipline. |
| **Close sign** | architect | Single-sign per §8C.2; caught stale refs + scope drift multiple times |

**Why sw plans (not architect):**
- Sw knows what's feasible + what frameworks exist + what shortcuts are safe
- Architect-drafted plans miss implementation realities (catch up via reviewer cycle anyway)
- Sw self-commits to scope they can deliver — fewer revision cycles
- Architect ratifies SCOPE (caught 2 binding clarifications at G5-4 v0.2→v0.3) but doesn't need to author implementation detail

**Why QA reviews + verifies (doesn't plan):**
- Independent third party; not advocate for either scope or implementation choice
- Catches discipline gaps (invariant inscription, acceptance criteria coverage) sw + architect miss
- Owns m01 hardware verification + component scenarios

**Edge case — process changes (this doc):** QA proposes; architect signs (same as any scope-affecting change).

**Edge case — hotfix-class** (per §6.3): sw can self-author + self-merge with QA spot-review. Architect ratifies if invariant-affecting.
