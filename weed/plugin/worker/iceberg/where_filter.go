package iceberg

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

var (
	whereEqualsPattern = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	whereInPattern     = regexp.MustCompile(`^(?i)([A-Za-z_][A-Za-z0-9_]*)\s+IN\s*\((.*)\)$`)
)

type whereClause struct {
	Field    string
	Literals []string
}

type partitionPredicate struct {
	Clauses []whereClause
}

func validateWhereOperations(where string, ops []string) error {
	if strings.TrimSpace(where) == "" {
		return nil
	}
	for _, op := range ops {
		switch op {
		case "compact", "rewrite_manifests", "rewrite_position_delete_files":
			continue
		default:
			return fmt.Errorf("where filter is only supported for compact, rewrite_position_delete_files, and rewrite_manifests")
		}
	}
	return nil
}

func parsePartitionPredicate(where string, meta table.Metadata) (*partitionPredicate, error) {
	where = strings.TrimSpace(where)
	if where == "" {
		return nil, nil
	}
	if meta == nil {
		return nil, fmt.Errorf("where filter requires table metadata")
	}

	specs := meta.PartitionSpecs()
	if len(specs) == 0 || meta.PartitionSpec().IsUnpartitioned() {
		return nil, fmt.Errorf("where filter is not supported for unpartitioned tables")
	}

	rawClauses := splitWhereConjunction(where)
	clauses := make([]whereClause, 0, len(rawClauses))
	for _, raw := range rawClauses {
		clause, err := parseWhereClause(raw)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}

	for _, clause := range clauses {
		for _, spec := range specs {
			if !specHasFieldByName(spec, clause.Field) {
				return nil, fmt.Errorf("where field %q is not present in partition spec %d", clause.Field, spec.ID())
			}
		}
	}

	return &partitionPredicate{Clauses: clauses}, nil
}

func splitWhereConjunction(where string) []string {
	parts := regexp.MustCompile(`(?i)\s+AND\s+`).Split(where, -1)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseWhereClause(raw string) (whereClause, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return whereClause{}, fmt.Errorf("empty where clause")
	}
	if matches := whereInPattern.FindStringSubmatch(raw); matches != nil {
		literals, err := splitLiteralList(matches[2])
		if err != nil {
			return whereClause{}, err
		}
		if len(literals) == 0 {
			return whereClause{}, fmt.Errorf("empty IN list in where clause %q", raw)
		}
		return whereClause{Field: matches[1], Literals: literals}, nil
	}
	if matches := whereEqualsPattern.FindStringSubmatch(raw); matches != nil {
		return whereClause{Field: matches[1], Literals: []string{strings.TrimSpace(matches[2])}}, nil
	}
	return whereClause{}, fmt.Errorf("unsupported where clause %q", raw)
}

func splitLiteralList(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var (
		literals []string
		current  strings.Builder
		quote    rune
	)
	for _, r := range raw {
		switch {
		case quote != 0:
			current.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
			current.WriteRune(r)
		case r == ',':
			literal := strings.TrimSpace(current.String())
			if literal != "" {
				literals = append(literals, literal)
			}
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted literal in IN list")
	}
	if literal := strings.TrimSpace(current.String()); literal != "" {
		literals = append(literals, literal)
	}
	return literals, nil
}

func specHasFieldByName(spec iceberg.PartitionSpec, fieldName string) bool {
	for field := range spec.Fields() {
		if field.Name == fieldName {
			return true
		}
	}
	return false
}

func specByID(meta table.Metadata) map[int]iceberg.PartitionSpec {
	result := make(map[int]iceberg.PartitionSpec)
	if meta == nil {
		return result
	}
	for _, spec := range meta.PartitionSpecs() {
		result[spec.ID()] = spec
	}
	return result
}

func (p *partitionPredicate) Matches(spec iceberg.PartitionSpec, partition map[int]any) (bool, error) {
	if p == nil {
		return true, nil
	}

	valuesByName := make(map[string]any)
	for field := range spec.Fields() {
		if value, ok := partition[field.FieldID]; ok {
			valuesByName[field.Name] = value
			continue
		}
		valuesByName[field.Name] = partition[field.SourceID]
	}

	for _, clause := range p.Clauses {
		actual, ok := valuesByName[clause.Field]
		if !ok {
			return false, fmt.Errorf("partition field %q missing from spec %d", clause.Field, spec.ID())
		}
		matched := false
		for _, literal := range clause.Literals {
			ok, err := literalMatchesActual(literal, actual)
			if err != nil {
				return false, fmt.Errorf("where field %q: %w", clause.Field, err)
			}
			if ok {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func literalMatchesActual(raw string, actual any) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, fmt.Errorf("empty literal")
	}

	switch v := actual.(type) {
	case string:
		value, err := unquoteLiteral(raw)
		if err != nil {
			return false, err
		}
		return v == value, nil
	case bool:
		value, err := strconv.ParseBool(strings.ToLower(strings.TrimSpace(raw)))
		if err != nil {
			return false, fmt.Errorf("parse bool literal %q: %w", raw, err)
		}
		return v == value, nil
	case int:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return false, fmt.Errorf("parse int literal %q: %w", raw, err)
		}
		return int64(v) == value, nil
	case int32:
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return false, fmt.Errorf("parse int32 literal %q: %w", raw, err)
		}
		return v == int32(value), nil
	case int64:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return false, fmt.Errorf("parse int64 literal %q: %w", raw, err)
		}
		return v == value, nil
	case float32:
		value, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return false, fmt.Errorf("parse float32 literal %q: %w", raw, err)
		}
		return v == float32(value), nil
	case float64:
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return false, fmt.Errorf("parse float64 literal %q: %w", raw, err)
		}
		return v == value, nil
	default:
		value, err := unquoteLiteral(raw)
		if err != nil {
			return false, err
		}
		return fmt.Sprint(actual) == value, nil
	}
}

func unquoteLiteral(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		if (raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"') {
			return raw[1 : len(raw)-1], nil
		}
	}
	return raw, nil
}
