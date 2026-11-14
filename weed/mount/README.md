Concurrent writers to one file from different mounts require `-dlm`; without it, a conflicting write fails with EIO and the file may refuse further appends until reopened after the cache drops.
