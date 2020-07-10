package filesys

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirPath(t *testing.T) {

	p := &Dir{name: "/some"}
	p = &Dir{name: "path", parent: p}
	p = &Dir{name: "to", parent: p}
	p = &Dir{name: "a", parent: p}
	p = &Dir{name: "file", parent: p}

	assert.Equal(t, "/some/path/to/a/file", p.FullPath())

	p = &Dir{name: "/some"}
	assert.Equal(t, "/some", p.FullPath())

	p = &Dir{name: "/"}
	assert.Equal(t, "/", p.FullPath())

	p = &Dir{name: "/"}
	p = &Dir{name: "path", parent: p}
	assert.Equal(t, "/path", p.FullPath())

	p = &Dir{name: "/"}
	p = &Dir{name: "path", parent: p}
	p = &Dir{name: "to", parent: p}
	assert.Equal(t, "/path/to", p.FullPath())

}
