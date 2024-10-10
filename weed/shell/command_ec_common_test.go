package shell

import (
	"fmt"
	"testing"
)

func Test_ceilDivide(t *testing.T) {
	//3
	result1 := ceilDivide(14, 5)
	fmt.Println(result1)

	//5
	result2 := ceilDivide(14, 3)
	fmt.Println(result2)

	//2
	result3 := ceilDivide(14, 7)
	fmt.Println(result3)
}
