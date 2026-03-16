package iceberg

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
)

type compactionRewritePlan struct {
	strategy   string
	sortFields []compactionSortField
}

type compactionSortField struct {
	path       string
	descending bool
	nullsFirst bool
}

func resolveCompactionRewritePlan(config Config, meta table.Metadata) (*compactionRewritePlan, error) {
	strategy := strings.TrimSpace(strings.ToLower(config.RewriteStrategy))
	if strategy == "" {
		strategy = defaultRewriteStrategy
	}
	if strategy == defaultRewriteStrategy {
		return &compactionRewritePlan{strategy: defaultRewriteStrategy}, nil
	}
	if strategy != "sort" {
		return nil, fmt.Errorf("unsupported rewrite strategy %q", config.RewriteStrategy)
	}

	sortFields, err := resolveCompactionSortFields(config, meta)
	if err != nil {
		return nil, err
	}

	return &compactionRewritePlan{
		strategy:   strategy,
		sortFields: sortFields,
	}, nil
}

func resolveCompactionSortFields(config Config, meta table.Metadata) ([]compactionSortField, error) {
	if meta == nil || meta.CurrentSchema() == nil {
		return nil, fmt.Errorf("sort rewrite requires table schema metadata")
	}
	if strings.TrimSpace(config.SortFields) != "" {
		return parseConfiguredCompactionSortFields(meta.CurrentSchema(), config.SortFields)
	}

	sortOrder := meta.SortOrder()
	if sortOrder.IsUnsorted() {
		return nil, fmt.Errorf("sort rewrite requires sort_fields or a table sort order")
	}

	schema := meta.CurrentSchema()
	seen := make(map[string]struct{})
	fields := make([]compactionSortField, 0, sortOrder.Len())
	for sortField := range sortOrder.Fields() {
		if _, ok := sortField.Transform.(iceberg.IdentityTransform); !ok {
			return nil, fmt.Errorf("table sort field %d uses unsupported transform %q", sortField.SourceID, sortField.Transform)
		}

		field, ok := schema.FindFieldByID(sortField.SourceID)
		if !ok {
			return nil, fmt.Errorf("table sort field %d not found in schema", sortField.SourceID)
		}
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, fmt.Errorf("table sort field %q is not a primitive column", field.Name)
		}

		columnPath, ok := schema.FindColumnName(sortField.SourceID)
		if !ok {
			columnPath = field.Name
		}
		normalizedPath := strings.ToLower(columnPath)
		if _, exists := seen[normalizedPath]; exists {
			return nil, fmt.Errorf("duplicate sort field %q", columnPath)
		}
		seen[normalizedPath] = struct{}{}

		fields = append(fields, compactionSortField{
			path:       columnPath,
			descending: sortField.Direction == table.SortDESC,
			nullsFirst: sortField.NullOrder == table.NullsFirst,
		})
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("sort rewrite requires at least one sort field")
	}
	return fields, nil
}

func parseConfiguredCompactionSortFields(schema *iceberg.Schema, raw string) ([]compactionSortField, error) {
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{})
	fields := make([]compactionSortField, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		segments := strings.Split(part, ":")
		fieldName := strings.TrimSpace(segments[0])
		if fieldName == "" {
			return nil, fmt.Errorf("sort field name cannot be empty")
		}

		descending := false
		switch len(segments) {
		case 1:
		default:
			switch strings.ToLower(strings.TrimSpace(segments[1])) {
			case "", "asc":
				descending = false
			case "desc":
				descending = true
			default:
				return nil, fmt.Errorf("invalid sort direction %q for %q", segments[1], fieldName)
			}
		}

		nullsFirst := !descending
		if len(segments) > 2 {
			switch strings.ToLower(strings.TrimSpace(segments[2])) {
			case "nulls-first":
				nullsFirst = true
			case "nulls-last":
				nullsFirst = false
			default:
				return nil, fmt.Errorf("invalid null order %q for %q", segments[2], fieldName)
			}
		}
		if len(segments) > 3 {
			return nil, fmt.Errorf("invalid sort field %q", part)
		}

		field, ok := schema.FindFieldByNameCaseInsensitive(fieldName)
		if !ok {
			return nil, fmt.Errorf("sort field %q not found in schema", fieldName)
		}
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, fmt.Errorf("sort field %q is not a primitive column", fieldName)
		}

		columnPath, ok := schema.FindColumnName(field.ID)
		if !ok {
			columnPath = field.Name
		}
		normalizedPath := strings.ToLower(columnPath)
		if _, exists := seen[normalizedPath]; exists {
			return nil, fmt.Errorf("duplicate sort field %q", columnPath)
		}
		seen[normalizedPath] = struct{}{}

		fields = append(fields, compactionSortField{
			path:       columnPath,
			descending: descending,
			nullsFirst: nullsFirst,
		})
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("sort rewrite requires at least one sort field")
	}
	return fields, nil
}

func filterCompactionBinsByPlan(bins []compactionBin, config Config, plan *compactionRewritePlan) []compactionBin {
	if plan == nil || plan.strategy != "sort" || config.SortMaxInputBytes <= 0 {
		return bins
	}

	filtered := make([]compactionBin, 0, len(bins))
	for _, bin := range bins {
		if bin.TotalSize <= config.SortMaxInputBytes {
			filtered = append(filtered, bin)
		}
	}
	return filtered
}

func compactionNoEligibleMessage(config Config, plan *compactionRewritePlan, initialBinCount int) string {
	if plan != nil && plan.strategy == "sort" && config.SortMaxInputBytes > 0 && initialBinCount > 0 {
		return fmt.Sprintf("no files eligible for sorted compaction within %d MB sort input cap", config.SortMaxInputBytes/(1024*1024))
	}
	return "no files eligible for compaction"
}

func (p *compactionRewritePlan) summaryLabel() string {
	if p == nil || p.strategy != "sort" {
		return defaultRewriteStrategy
	}

	paths := make([]string, 0, len(p.sortFields))
	for _, field := range p.sortFields {
		label := field.path
		if field.descending {
			label += " desc"
		} else {
			label += " asc"
		}
		if field.nullsFirst {
			label += " nulls-first"
		} else {
			label += " nulls-last"
		}
		paths = append(paths, label)
	}
	return fmt.Sprintf("sort (%s)", strings.Join(paths, ", "))
}

func (p *compactionRewritePlan) parquetSortingColumns(pqSchema *parquet.Schema) ([]parquet.SortingColumn, error) {
	if p == nil || p.strategy != "sort" {
		return nil, nil
	}
	if pqSchema == nil {
		return nil, fmt.Errorf("parquet schema is required for sort rewrite")
	}

	columns := make([]parquet.SortingColumn, 0, len(p.sortFields))
	for _, field := range p.sortFields {
		path := strings.Split(field.path, ".")
		if _, ok := pqSchema.Lookup(path...); !ok {
			return nil, fmt.Errorf("parquet schema missing sort field %q", field.path)
		}

		var column parquet.SortingColumn
		if field.descending {
			column = parquet.Descending(path...)
		} else {
			column = parquet.Ascending(path...)
		}
		if field.nullsFirst {
			column = parquet.NullsFirst(column)
		}
		columns = append(columns, column)
	}
	return columns, nil
}
