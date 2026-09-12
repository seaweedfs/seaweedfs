package postgres_s3

/*
 * Copyright 2022 Splunk Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculatePrefixes(t *testing.T) {
	assert := assert.New(t)
	var path string
	var prefixes []string

	path = "/test1"
	prefixes = calculatePrefixes(path)
	assert.Equal(prefixes, []string(nil))

	path = "/test1/test2"
	prefixes = calculatePrefixes(path)
	assert.Equal(prefixes, []string{"/test1/"})

	path = "/test1/test2/test3"
	prefixes = calculatePrefixes(path)
	assert.Equal(prefixes, []string{"/test1/", "/test1/test2/"})
}
