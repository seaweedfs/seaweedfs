package services

import (
    "testing"
    "github.com/stretchr/testify/assert"
)

func TestGetAll(t *testing.T) {
	concatenatedNames := ""
	for _, name := range GetAll() {
		concatenatedNames += name.String()
	}

    assert.Equal(t, concatenatedNames, _Name_name)
}
