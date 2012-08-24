package storage

import (
    "testing"
)

func TestReadStorageLimit(t *testing.T) {
  sl := NewStorageLimit(1000)
  println("detected:",sl.detectedLimit)
}

