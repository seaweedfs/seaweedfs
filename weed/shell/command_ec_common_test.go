package shell

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ceilDivide(t *testing.T) {
	ast := assert.New(t)
	//3
	ast.Equal(3, ceilDivide(14, 5))

	//5
	ast.Equal(5, ceilDivide(14, 3))

	//2
	ast.Equal(2, ceilDivide(14, 7))
}
