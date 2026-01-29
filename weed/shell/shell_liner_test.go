package shell

import (
	"flag"
	"reflect"
	"regexp"
	"testing"
)

func TestRegexSplitting(t *testing.T) {
	// New regex
	reg, _ := regexp.Compile(`(?:"[^"]*"|'[^']*'|[^\s"'])+`)

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
			expected: []string{`s3.configure`, `-user=Test_number_004`, `-account_display_name="Test number 004"`, `-actions=write`, `-apply`},
		},
		{
			input:    `s3.configure -user=Test_number_004 -account_display_name='Test number 004' -actions=write -apply`,
			expected: []string{`s3.configure`, `-user=Test_number_004`, `-account_display_name='Test number 004'`, `-actions=write`, `-apply`},
		},
		{
			input:    `s3.configure -flag="a b"c'd e'`,
			expected: []string{`s3.configure`, `-flag="a b"c'd e'`},
		},
	}

	for _, tt := range tests {
		got := reg.FindAllString(tt.input, -1)
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
