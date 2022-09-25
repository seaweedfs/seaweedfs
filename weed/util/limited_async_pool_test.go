package util

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
)

func TestAsyncPool(t *testing.T) {
	p := NewLimitedAsyncExecutor(3)
	var results []Future

	results = append(results, p.Execute(FirstFunc))
	results = append(results, p.Execute(SecondFunc))
	results = append(results, p.Execute(ThirdFunc))
	results = append(results, p.Execute(FourthFunc))
	results = append(results, p.Execute(FifthFunc))

	var sorted_results []int
	for _, r := range results {
		x := r.Await().(int)
		println(x)
		sorted_results = append(sorted_results, x)
	}
	assert.True(t, sort.IntsAreSorted(sorted_results), "results should be sorted")
}

func FirstFunc() any {
	fmt.Println("-- Executing first function --")
	time.Sleep(70 * time.Millisecond)
	fmt.Println("-- First Function finished --")
	return 1
}

func SecondFunc() any {
	fmt.Println("-- Executing second function --")
	time.Sleep(50 * time.Millisecond)
	fmt.Println("-- Second Function finished --")
	return 2
}

func ThirdFunc() any {
	fmt.Println("-- Executing third function --")
	time.Sleep(20 * time.Millisecond)
	fmt.Println("-- Third Function finished --")
	return 3
}

func FourthFunc() any {
	fmt.Println("-- Executing fourth function --")
	time.Sleep(100 * time.Millisecond)
	fmt.Println("-- Fourth Function finished --")
	return 4
}

func FifthFunc() any {
	fmt.Println("-- Executing fifth function --")
	time.Sleep(40 * time.Millisecond)
	fmt.Println("-- Fourth fifth finished --")
	return 5
}
