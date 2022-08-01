package json

import (
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
)

type Query struct {
	Field string
	Op    string
	Value string
}

func QueryJson(jsonLine string, projections []string, query Query) (passedFilter bool, values []sqltypes.Value) {
	if filterJson(jsonLine, query) {
		passedFilter = true
		fields := gjson.GetMany(jsonLine, projections...)
		for _, f := range fields {
			values = append(values, sqltypes.MakeTrusted(sqltypes.Type(f.Type), sqltypes.StringToBytes(f.Raw)))
		}
		return
	}
	return false, nil
}

func filterJson(jsonLine string, query Query) bool {

	value := gjson.Get(jsonLine, query.Field)

	// copied from gjson.go queryMatches() function
	rpv := query.Value

	if !value.Exists() {
		return false
	}
	if query.Op == "" {
		// the query is only looking for existence, such as:
		//   friends.#(name)
		// which makes sure that the array "friends" has an element of
		// "name" that exists
		return true
	}
	switch value.Type {
	case gjson.String:
		switch query.Op {
		case "=":
			return value.Str == rpv
		case "!=":
			return value.Str != rpv
		case "<":
			return value.Str < rpv
		case "<=":
			return value.Str <= rpv
		case ">":
			return value.Str > rpv
		case ">=":
			return value.Str >= rpv
		case "%":
			return match.Match(value.Str, rpv)
		case "!%":
			return !match.Match(value.Str, rpv)
		}
	case gjson.Number:
		rpvn, _ := strconv.ParseFloat(rpv, 64)
		switch query.Op {
		case "=":
			return value.Num == rpvn
		case "!=":
			return value.Num != rpvn
		case "<":
			return value.Num < rpvn
		case "<=":
			return value.Num <= rpvn
		case ">":
			return value.Num > rpvn
		case ">=":
			return value.Num >= rpvn
		}
	case gjson.True:
		switch query.Op {
		case "=":
			return rpv == "true"
		case "!=":
			return rpv != "true"
		case ">":
			return rpv == "false"
		case ">=":
			return true
		}
	case gjson.False:
		switch query.Op {
		case "=":
			return rpv == "false"
		case "!=":
			return rpv != "false"
		case "<":
			return rpv == "true"
		case "<=":
			return true
		}
	}
	return false

}
