package weed_server

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/ptrie"
	"testing"
)

func TestPtrie(t *testing.T) {
	b := []byte("/topics/abc/dev")
	excludedTrie := ptrie.New[bool]()
	excludedTrie.Put([]byte("/topics/abc/d"), true)
	excludedTrie.Put([]byte("/topics/abc"), true)

	assert.True(t, excludedTrie.MatchPrefix(b, func(key []byte, value bool) bool {
		println("matched1", string(key))
		return true
	}))

	assert.True(t, excludedTrie.MatchAll(b, func(key []byte, value bool) bool {
		println("matched2", string(key))
		return true
	}))

	assert.False(t, excludedTrie.MatchAll([]byte("/topics/ab"), func(key []byte, value bool) bool {
		println("matched3", string(key))
		return true
	}))

	assert.False(t, excludedTrie.Has(b))
}
