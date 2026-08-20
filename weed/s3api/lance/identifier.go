package lance

import (
	"fmt"
	"net/http"
	"strings"
)

// defaultDelimiter joins the parts of a Lance string identifier when the caller
// does not pass ?delimiter=. An id equal to the delimiter is the root namespace,
// so /v1/namespace/$/list lists the root's children.
const defaultDelimiter = "$"

// identifier is a Lance object identifier: zero parts is the root namespace, one
// part names a table bucket, and the rest are namespace parts with a table name
// last. The mapping onto storage is bucket / namespace / table, which is the
// three-level shape Lance clients already use.
type identifier []string

func requestDelimiter(r *http.Request) string {
	if d := r.URL.Query().Get("delimiter"); d != "" {
		return d
	}
	return defaultDelimiter
}

// parseIdentifier decodes the {id} route variable. Empty parts are rejected
// rather than dropped so that "a$$b" cannot silently resolve to "a$b".
func parseIdentifier(encoded, delimiter string) (identifier, error) {
	if encoded == "" || encoded == delimiter {
		return nil, nil
	}
	parts := strings.Split(encoded, delimiter)
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("identifier %q has an empty part", encoded)
		}
	}
	return parts, nil
}

func (id identifier) String(delimiter string) string {
	if len(id) == 0 {
		return delimiter
	}
	return strings.Join(id, delimiter)
}

// namespace splits a namespace identifier into the table bucket and the parts of
// the namespace inside it. The root and a bare bucket both return an empty
// namespace, so callers check the bucket to tell them apart.
func (id identifier) namespace() (bucket string, ns []string) {
	if len(id) == 0 {
		return "", nil
	}
	return id[0], id[1:]
}

// table splits a table identifier. A table needs a bucket, at least one
// namespace part and a name, because storage has no unnamespaced tables.
func (id identifier) table() (bucket string, ns []string, name string, err error) {
	if len(id) < 3 {
		return "", nil, "", fmt.Errorf("table identifier needs a bucket, a namespace and a name")
	}
	return id[0], id[1 : len(id)-1], id[len(id)-1], nil
}

// matchesBody reports whether an identifier carried in the request body agrees
// with the one in the route. The spec requires 400 when both are present and
// differ, and requires the route to win when the body omits it.
func (id identifier) matchesBody(body []string) bool {
	if len(body) == 0 {
		return true
	}
	if len(body) != len(id) {
		return false
	}
	for i := range body {
		if body[i] != id[i] {
			return false
		}
	}
	return true
}
