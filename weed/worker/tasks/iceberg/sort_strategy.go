package iceberg

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/glog"
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

	sortFields, err := resolveCompactionSortFields(meta)
	if err != nil {
		if err == errUnsupportedTableSortOrder {
			glog.V(2).Infof("iceberg compact: falling back to binpack because the table sort order is not supported")
			return &compactionRewritePlan{strategy: defaultRewriteStrategy}, nil
		}
		return nil, err
	}

	return &compactionRewritePlan{
		strategy:   strategy,
		sortFields: sortFields,
	}, nil
}

var errUnsupportedTableSortOrder = fmt.Errorf("unsupported table sort order")

func resolveCompactionSortFields(meta table.Metadata) ([]compactionSortField, error) {
	if meta == nil || meta.CurrentSchema() == nil {
		return nil, fmt.Errorf("sort rewrite requires table schema metadata")
	}

	sortOrder := meta.SortOrder()
	if sortOrder.IsUnsorted() {
		return nil, fmt.Errorf("sort rewrite requires a table sort order")
	}

	schema := meta.CurrentSchema()
	seen := make(map[string]struct{})
	fields := make([]compactionSortField, 0, sortOrder.Len())
	for sortField := range sortOrder.Fields() {
		if _, ok := sortField.Transform.(iceberg.IdentityTransform); !ok {
			return nil, errUnsupportedTableSortOrder
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

func compactionTargetSizeForPlan(config Config, plan *compactionRewritePlan) int64 {
	targetSize := config.TargetFileSizeBytes
	if plan != nil && plan.strategy == "sort" && config.SortMaxInputBytes > 0 && config.SortMaxInputBytes < targetSize {
		return config.SortMaxInputBytes
	}
	return targetSize
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
		return fmt.Sprintf("no files eligible for sorted compaction within %d MB sort input cap", config.SortMaxInputBytes/bytesPerMB)
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
