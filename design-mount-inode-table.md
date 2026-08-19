# Design: Keying the mount's inode table on parent and name

Issue #10020 — a mount over 32.7M files sits at 2.1GB RSS after the readdir fixes.
Its heap profile is 94% one structure: `InodeToPath`.

## Where the bytes are

Measured by populating the table with 1M children and reading `HeapAlloc` around
it, after the two cheap wins already landed:

    221.5 B/inode at  85-character paths
    285.6 B/inode at 148-character paths

so roughly **125 B fixed plus the full path rounded to a Go size class**. The
fixed part is `InodeEntry` (32) and the two maps (~93). Reading the reporter's
profile against that model — `Lookup` flat / 125 — gives ~5.6M live inodes at
~200-character paths, ~350 B each, which is the 2.0GB.

The path is the larger half and it grows with directory depth. It is stored once
and referenced twice: as the `path2inode` key and as `InodeEntry.path`.

## Shape

Directories keep a materialized full path. Files keep only their parent's inode
and their own name. There are orders of magnitude fewer directories than files,
so the full paths that remain are a rounding error, and no operation has to walk
a parent chain: a file's path is one concat off its parent's.

    type childKey struct {
        parent uint64
        name   string
    }

    type InodeEntry struct {   // 32 bytes, one object, no allocation of its own
        parent  uint64
        name    string
        nlookup uint64
    }

    inodes     map[uint64]*InodeEntry     // every inode
    children   map[childKey]uint64        // (parent, name) -> inode
    dirs       map[uint64]*dirState       // directories only; carries the path
    dirPaths   map[util.FullPath]uint64   // directories only
    extraLinks map[uint64][]childKey      // hard-linked inodes only

`InodeEntry.name` shares its bytes with the `children` key, exactly as the path
does with `path2inode` today.

The name has to be copied on the way in. Every name the mount has to hand is a
slice of something longer — `FullPath.Name()` slices the full path, and a
readdir's `dirEntry.Name` slices the listed entry's — and a Go string slice
keeps its whole backing array alive. Storing one as-is would pin the ~200-byte
path it came from per inode and leave the table costing what it costs today.
`strings.Clone` at the point of storage; it is one small allocation per new
inode, which is what building the child path costs already.

Per inode: entry 32, name bytes ~32, `children` ~51, `inodes` ~26 — about
**141 B, independent of path length**, against ~350 at the reporter's depth. At
5.6M inodes that is 2.0GB down to 0.8GB.

## What each operation becomes

`GetPath(inode)` — root is 1; a directory returns `dirs[inode].path`; anything
else is `dirs[e.parent].path.Child(e.name)`. O(1), one allocation. Today it
returns an existing string and allocates nothing, so this is the trade: one
~200-byte allocation on an op that is already about to talk to a filer. It is
per FUSE op, not per readdir entry.

`GetInode(path)` — `dirPaths[path]`, else split and hit `children`. Two map
lookups, no walk. It keeps taking a path, so its thirteen callers are untouched
by any of this; only its body changes.

`Lookup` — the signature changes from a full path to `(parentInode, name)`. All
nine call sites already hold the parent nodeid; the readdir loop stops building
a child path per entry entirely. Only the key shape changes: reusing the inode
already at that key, honouring `possibleInode`, probing past a collision for
anything that is not a hard link, and counting `nlookup` only when `isLookup`
all have to survive intact, and want tests of their own before the switch.

The directory-state methods (`InvalidateChildrenCache`, `TouchDirectory`,
`MarkChildrenCached`, `AdjustSubdirCount`, ~30 call sites between them) resolve a
directory by path and stay exactly as they are, against `dirPaths`.

`Forget` — unchanged in how it counts: it subtracts the request's count from
`nlookup`, keeps every mapping while that stays above zero, and clamps a kernel
over-decrement the way it does today. At zero it drops every one of the inode's
child keys, not just the primary:
`children[{parent,name}]` plus each key in `extraLinks`, then `inodes`, and for
a directory `dirs` and `dirPaths`. Unlinking one link of a hard-linked file
promotes a surviving key into `InodeEntry`, the same shape `removeOnePath` has
today, so `GetPath` never answers with a name that is gone.

A directory has to outlive its children, since that is what resolves them.
`dirState` carries a child count, incremented as `children` gains a key under
that parent and decremented as one goes; `Forget` releases a directory only at
nlookup 0 **and** count 0, and re-checks on the last child's release. Linux
evicts dentries bottom-up so this should never fire, which is the reason to
assert it rather than assume it.

## What it does to rename

Renaming a directory today is repaired entry by entry. The filer moves every
descendant and emits an event for each, and the mount turns each one into a
`MovePath`, because every cached path under the directory holds the old prefix
as literal bytes. A rename of a directory with a million cached children is a
million table mutations, and the table is only correct as long as every one of
those events arrives and is applied.

Keyed on the parent, files under a renamed directory need nothing at all — their
key never mentioned the prefix. Only descendant *directories* have to have their
materialized path re-prefixed, and there are few of those.

`MovePath` today moves the renamed entry and nothing under it, which is visible
directly against the table (this is current behavior, not the target):

    itp := NewInodeToPath("/", 0)
    dirInode := itp.Lookup("/a", now, true, false, 0, true)
    childInode := itp.Lookup("/a/f.txt", now, false, false, 0, true)
    itp.MovePath("/a", "/b")
    itp.GetPath(childInode)   // "/a/f.txt"

The per-descendant events are what keep that from being reachable in practice.

## Phases

Each is its own commit, and the table's behavior is unchanged until the second.

1. Materialize the directory path in `dirState`, add `dirPaths`, serve directory
   `GetPath` from it. Establishes the invariant and its tests with the file half
   untouched.
2. `InodeEntry` becomes `(parent, name)` and `extraLinks` arrives with it;
   `path2inode` becomes `children`; `GetPath` and `GetInode` take their new
   form, and `MovePath` re-prefixes the materialized path of every descendant
   directory, with a regression test that walks a nested tree through both a
   local and a subscription-driven rename. The load-bearing commit. A
   hard-linked file has to keep every one of its names from this commit on, not
   from phase 4 — a phase that is individually correct cannot park them
   somewhere in between.
3. Convert `Lookup` and its nine callers to `(parentInode, name)`. It keeps its
   path signature through phase 2, so that commit does not have to move both
   halves at once.
4. Tighten `InodeEntry` back to the 32-byte size class and assert it.
5. Shed entries the mount no longer wants to hold. See below.

## Bounding the table

Everything above lowers the constant. Growth is still unbounded, because the
only thing that ever removes an entry is a kernel FORGET, which arrives when the
kernel evicts the dentry — under dcache pressure, on unlink, or at unmount. A
machine with memory to spare never sends one.

Two halves to that, and only one is ours to refuse. A readdirplus reference is
speculative: the mount hands the kernel attributes for every name it lists,
whether or not the client ever looks at them. That one can be declined by
leaving the `EntryOut` zeroed — Linux reads a zero nodeid in a READDIRPLUS reply
as "no attributes for this entry" (`fuse_direntplus_link`) and takes no
reference. A LOOKUP reference cannot be declined: a zero nodeid there means
ENOENT, which would be a lie about the file existing.

So a cap on its own only stops the mount paying for attributes nobody asked for,
and never shrinks anything. Actually shedding entries means asking the kernel to
drop the dentry with `EntryNotify(parent, name)`, from the invalidation worker
and never from a request handler — the deadlock `invalidateKernelDirListing`
already documents. That call wants exactly the pair phase 2 makes the table's
key, which is why this comes last: an eviction sweep is cheap to write against
`children` and awkward against a map of full paths. It names one dentry, so a
hard-linked inode needs one call per name — the primary and every key in
`extraLinks` — before the entry goes, or the kernel keeps the aliases it was
never told about.

## Risks

A child must never outlive its parent in the table, or its path is
unresolvable. Linux evicts dentries bottom-up so FORGET arrives child-first, but
the table should refuse to release a directory that still has children rather
than trust it.

Every rename path has to keep `dirs`/`dirPaths` in step, including the ones that
arrive from the metadata subscription rather than from a local syscall.

A rename this mount never sees — a subscription gap across a reconnect — strands
a stale path either way. Fewer entries hold one, since a file's key never
mentioned the prefix, but that buys nothing at resolution time: a file resolves
through `dirs[e.parent].path`, so one stale directory path is enough to make
every file under it resolve stale too. The exposure is the same shape as today's
and reconciling the table against the filer after a gap is separate work.
