package json

import "github.com/seaweedfs/seaweedfs/weed/query/sqltypes"

func ToJson(buf []byte, selections []string, values []sqltypes.Value) []byte {
	buf = append(buf, '{')
	for i, value := range values {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, selections[i]...)
		buf = append(buf, ':')
		buf = append(buf, value.Raw()...)
	}
	buf = append(buf, '}')
	return buf
}
