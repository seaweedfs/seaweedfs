package shell

import (
	"reflect"
	"testing"
)

func TestParseShardsSpec_Range(t *testing.T) {
	got, err := parseShardsSpec("3-7")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := []int{3, 4, 5, 6, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseShardsSpec_Set(t *testing.T) {
	got, err := parseShardsSpec("0,3,7")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := []int{0, 3, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseShardsSpec_DedupSort(t *testing.T) {
	got, err := parseShardsSpec("7,3,3,0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := []int{0, 3, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseShardsSpec_OutOfRange(t *testing.T) {
	if _, err := parseShardsSpec("16"); err == nil {
		t.Fatal("expected out-of-range error for 16")
	}
	if _, err := parseShardsSpec("-1"); err == nil {
		t.Fatal("expected out-of-range error for -1")
	}
	if _, err := parseShardsSpec("0-16"); err == nil {
		t.Fatal("expected out-of-range error for 0-16")
	}
}

func TestParseShardsSpec_BadRange(t *testing.T) {
	if _, err := parseShardsSpec("7-3"); err == nil {
		t.Fatal("expected lo>hi error")
	}
}

func TestResolveShardSelection_Mutex(t *testing.T) {
	if _, err := resolveShardSelection(0, "1,2"); err == nil {
		t.Fatal("expected mutex error when both -shard and -shards set")
	}
	if _, err := resolveShardSelection(-1, ""); err == nil {
		t.Fatal("expected error when neither set")
	}
}

func TestResolveShardSelection_SingleShard(t *testing.T) {
	got, err := resolveShardSelection(5, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !reflect.DeepEqual(got, []int{5}) {
		t.Fatalf("got %v, want [5]", got)
	}
}

func TestFormatShardLabel(t *testing.T) {
	cases := []struct {
		in   []int
		want string
	}{
		{[]int{5}, "5"},
		{[]int{0, 1, 2, 3}, "0-3"},
		{[]int{0, 2, 5}, "0,2,5"},
	}
	for _, tc := range cases {
		if got := formatShardLabel(tc.in); got != tc.want {
			t.Errorf("formatShardLabel(%v)=%q, want %q", tc.in, got, tc.want)
		}
	}
}
