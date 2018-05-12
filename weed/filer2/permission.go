package filer2

func hasWritePermission(dir *Entry, entry *Entry) bool {

	if dir == nil {
		return false
	}

	if dir.Uid == entry.Uid && dir.Mode&0200 > 0 {
		return true
	}

	if dir.Gid == entry.Gid && dir.Mode&0020 > 0 {
		return true
	}

	if dir.Mode&0002 > 0 {
		return true
	}

	return false
}
