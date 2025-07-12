package dash

// FormatFileMode converts file mode to Unix-style string representation (e.g., "drwxr-xr-x")
// Handles both Go's os.ModeDir format and standard Unix file type bits
func FormatFileMode(mode uint32) string {
	var result []byte = make([]byte, 10)

	// File type - handle Go's os.ModeDir first, then standard Unix file type bits
	if mode&0x80000000 != 0 { // Go's os.ModeDir (0x80000000 = 2147483648)
		result[0] = 'd'
	} else {
		switch mode & 0170000 { // S_IFMT mask
		case 0040000: // S_IFDIR
			result[0] = 'd'
		case 0100000: // S_IFREG
			result[0] = '-'
		case 0120000: // S_IFLNK
			result[0] = 'l'
		case 0020000: // S_IFCHR
			result[0] = 'c'
		case 0060000: // S_IFBLK
			result[0] = 'b'
		case 0010000: // S_IFIFO
			result[0] = 'p'
		case 0140000: // S_IFSOCK
			result[0] = 's'
		default:
			result[0] = '-' // S_IFREG is default
		}
	}

	// Permission bits (always use the lower 12 bits regardless of file type format)
	// Owner permissions
	if mode&0400 != 0 { // S_IRUSR
		result[1] = 'r'
	} else {
		result[1] = '-'
	}
	if mode&0200 != 0 { // S_IWUSR
		result[2] = 'w'
	} else {
		result[2] = '-'
	}
	if mode&0100 != 0 { // S_IXUSR
		result[3] = 'x'
	} else {
		result[3] = '-'
	}

	// Group permissions
	if mode&0040 != 0 { // S_IRGRP
		result[4] = 'r'
	} else {
		result[4] = '-'
	}
	if mode&0020 != 0 { // S_IWGRP
		result[5] = 'w'
	} else {
		result[5] = '-'
	}
	if mode&0010 != 0 { // S_IXGRP
		result[6] = 'x'
	} else {
		result[6] = '-'
	}

	// Other permissions
	if mode&0004 != 0 { // S_IROTH
		result[7] = 'r'
	} else {
		result[7] = '-'
	}
	if mode&0002 != 0 { // S_IWOTH
		result[8] = 'w'
	} else {
		result[8] = '-'
	}
	if mode&0001 != 0 { // S_IXOTH
		result[9] = 'x'
	} else {
		result[9] = '-'
	}

	return string(result)
}
