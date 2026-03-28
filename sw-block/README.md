# sw-block

Private WAL V2 and standalone block-service workspace.

Purpose:
- keep WAL V2 design/prototype work isolated from WAL V1 production code in `weed/storage/blockvol`
- allow private design notes and experiments to evolve without polluting V1 delivery paths
- keep the future standalone `sw-block` product structure clean enough to split into a separate repo later if needed

Suggested layout:
- `design/`: shared V2 design docs
- `prototype/`: code prototypes and experiments
- `.private/`: private notes, phase development, roadmap, and non-public working material

Repository direction:
- current state: `sw-block/` is an isolated workspace inside `seaweedfs`
- likely future state: `sw-block` becomes a standalone sibling repo/product
- design and prototype structure should therefore stay product-oriented and not depend on SeaweedFS-specific paths
