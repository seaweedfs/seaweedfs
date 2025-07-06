package needle

type Version uint8

const (
	Version1 = Version(1)
	Version2 = Version(2)
	Version3 = Version(3)
)

func GetCurrentVersion() Version {
	return Version3
}
