# .private

Private working area for `sw-block`.

Use this for:
- phase development notes
- roadmap/progress tracking
- draft handoff notes
- temporary design comparisons
- prototype scratch work not ready for `design/` or `prototype/`

Recommended layout:
- `.private/phase/`: phase-by-phase development notes
- `.private/roadmap/`: short-term and medium-term execution notes
- `.private/handoff/`: notes for `sw`, `qa`, or future sessions

Phase protocol:
- each phase should normally have:
  - `phase-xx.md`
  - `phase-xx-log.md`
  - `phase-xx-decisions.md`
- details are defined in `.private/phase/README.md`

Promotion rules:
- stable vision/design docs go to `../design/`
- real prototype code stays in `../prototype/`
- `.private/` is for working material, not source of truth
