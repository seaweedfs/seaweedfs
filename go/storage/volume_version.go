package storage

type Version uint8

const (
	NoneVersion    = Version(0)
	Version1       = Version(1)
	Version2       = Version(2)
	CurrentVersion = Version2
)
