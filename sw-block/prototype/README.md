# V2 Prototype

Experimental WAL V2 prototype code lives here.

Current prototype:
- `fsmv2/`: pure in-memory replication FSM prototype
- `volumefsm/`: volume-level orchestrator prototype above `fsmv2`
- `distsim/`: early distributed/data-correctness simulator with synthetic 4K block values

Rules:
- do not wire this directly into WAL V1 production code
- keep interfaces and tests focused on architecture learning
- promote pieces into production only after V2 design stabilizes

## Windows test workflow

Because normal `go test` may be blocked by Windows Defender when it executes temporary test binaries from `%TEMP%`, use:

```powershell
powershell -ExecutionPolicy Bypass -File .\sw-block\prototype\run-tests.ps1
```

This builds test binaries into the workspace and runs them directly.
