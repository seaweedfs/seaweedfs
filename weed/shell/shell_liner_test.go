package shell

import (
	"flag"
	"reflect"
	"testing"
)

func TestSplitCommandLine(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    `s3.configure -user=test`,
			expected: []string{`s3.configure`, `-user=test`},
		},
		{
			input:    `s3.configure -user=Test_number_004 -account_display_name="Test number 004" -actions=write -apply`,
			expected: []string{`s3.configure`, `-user=Test_number_004`, `-account_display_name=Test number 004`, `-actions=write`, `-apply`},
		},
		{
			input:    `s3.configure -user=Test_number_004 -account_display_name='Test number 004' -actions=write -apply`,
			expected: []string{`s3.configure`, `-user=Test_number_004`, `-account_display_name=Test number 004`, `-actions=write`, `-apply`},
		},
		{
			input:    `s3.configure -flag="a b"c'd e'`,
			expected: []string{`s3.configure`, `-flag=a bcd e`},
		},
		{
			input:    `s3.configure -name="a\"b"`,
			expected: []string{`s3.configure`, `-name=a"b`},
		},
		{
			input:    `s3.configure -path='a\ b'`,
			expected: []string{`s3.configure`, `-path=a\ b`},
		},
	}

	for _, tt := range tests {
		got := splitCommandLine(tt.input)
		if !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("input: %s\ngot:  %v\nwant: %v", tt.input, got, tt.expected)
		}
	}
}

func TestStripQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{input: `"Test number 004"`, expected: `Test number 004`},
		{input: `'Test number 004'`, expected: `Test number 004`},
		{input: `-account_display_name="Test number 004"`, expected: `-account_display_name=Test number 004`},
		{input: `-flag="a"b'c'`, expected: `-flag=abc`},
		{input: `-name="a\"b"`, expected: `-name=a"b`},
		{input: `-path='a\ b'`, expected: `-path=a\ b`},
		{input: `"unbalanced`, expected: `"unbalanced`},
		{input: `'unbalanced`, expected: `'unbalanced`},
		{input: `-name="a\"b`, expected: `-name="a\"b`},
		{input: `trailing\`, expected: `trailing\`},
	}

	for _, tt := range tests {
		got := stripQuotes(tt.input)
		if got != tt.expected {
			t.Errorf("input: %s, got: %s, want: %s", tt.input, got, tt.expected)
		}
	}
}

func TestFlagParsing(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	displayName := fs.String("account_display_name", "", "display name")

	rawArg := `-account_display_name="Test number 004"`
	args := []string{stripQuotes(rawArg)}
	err := fs.Parse(args)
	if err != nil {
		t.Fatal(err)
	}

	expected := "Test number 004"
	if *displayName != expected {
		t.Errorf("got: [%s], want: [%s]", *displayName, expected)
	}
}

func TestEscapedFlagParsing(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	name := fs.String("name", "", "name")

	rawArg := `-name="a\"b"`
	args := []string{stripQuotes(rawArg)}
	err := fs.Parse(args)
	if err != nil {
		t.Fatal(err)
	}

	expected := `a"b`
	if *name != expected {
		t.Errorf("got: [%s], want: [%s]", *name, expected)
	}
}
