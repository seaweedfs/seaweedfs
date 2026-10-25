package plugin

// LockManager provides a shared exclusive lock for admin-managed detection/execution.
// Acquire returns a release function that must be called when the protected work finishes.
type LockManager interface {
	Acquire(reason string) (release func(), err error)
}
