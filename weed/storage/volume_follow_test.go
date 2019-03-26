package storage

import "testing"

func TestBinarySearch(t *testing.T) {
	var testInput []int
	testInput = []int{-1, 0, 3, 5, 9, 12}

	if 3 != binarySearchForLargerThanTarget(testInput, 4) {
		t.Errorf("failed to find target %d", 4)
	}
	if 3 != binarySearchForLargerThanTarget(testInput, 3) {
		t.Errorf("failed to find target %d", 3)
	}
	if 6 != binarySearchForLargerThanTarget(testInput, 12) {
		t.Errorf("failed to find target %d", 12)
	}
	if 1 != binarySearchForLargerThanTarget(testInput, -1) {
		t.Errorf("failed to find target %d", -1)
	}
	if 0 != binarySearchForLargerThanTarget(testInput, -2) {
		t.Errorf("failed to find target %d", -2)
	}

}

func binarySearchForLargerThanTarget(nums []int, target int) int {
	l := 0
	h := len(nums)
	for l < h {
		m := (l + h) / 2
		if nums[m] <= target {
			l = m + 1
		} else {
			h = m
		}
	}
	return l
}
