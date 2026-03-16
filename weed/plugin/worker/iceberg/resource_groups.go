package iceberg

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	resourceGroupNone            = "none"
	resourceGroupBucket          = "bucket"
	resourceGroupNamespace       = "namespace"
	resourceGroupBucketNamespace = "bucket_namespace"
)

type resourceGroupConfig struct {
	GroupBy           string
	MaxTablesPerGroup int64
}

func readResourceGroupConfig(values map[string]*plugin_pb.ConfigValue) (resourceGroupConfig, error) {
	groupBy := strings.TrimSpace(strings.ToLower(readStringConfig(values, "resource_group_by", "")))
	if groupBy == "" {
		groupBy = resourceGroupNone
	}

	switch groupBy {
	case resourceGroupNone, resourceGroupBucket, resourceGroupNamespace, resourceGroupBucketNamespace:
	default:
		return resourceGroupConfig{}, fmt.Errorf("invalid resource_group_by %q (valid: none, bucket, namespace, bucket_namespace)", groupBy)
	}

	maxTablesPerGroup := readInt64Config(values, "max_tables_per_resource_group", 0)
	if maxTablesPerGroup < 0 {
		return resourceGroupConfig{}, fmt.Errorf("max_tables_per_resource_group must be >= 0, got %d", maxTablesPerGroup)
	}
	if groupBy == resourceGroupNone && maxTablesPerGroup > 0 {
		return resourceGroupConfig{}, fmt.Errorf("max_tables_per_resource_group requires resource_group_by to be set")
	}

	return resourceGroupConfig{
		GroupBy:           groupBy,
		MaxTablesPerGroup: maxTablesPerGroup,
	}, nil
}

func (c resourceGroupConfig) enabled() bool {
	return c.GroupBy != "" && c.GroupBy != resourceGroupNone
}

func resourceGroupKey(info tableInfo, groupBy string) string {
	switch groupBy {
	case resourceGroupBucket:
		return info.BucketName
	case resourceGroupNamespace:
		return info.Namespace
	case resourceGroupBucketNamespace:
		return info.BucketName + "/" + info.Namespace
	default:
		return ""
	}
}

func selectTablesByResourceGroup(tables []tableInfo, cfg resourceGroupConfig, maxResults int) ([]tableInfo, bool) {
	if !cfg.enabled() {
		if maxResults > 0 && len(tables) > maxResults {
			return tables[:maxResults], true
		}
		return tables, false
	}

	grouped := make(map[string][]tableInfo)
	groupOrder := make([]string, 0)
	for _, table := range tables {
		key := resourceGroupKey(table, cfg.GroupBy)
		if _, ok := grouped[key]; !ok {
			groupOrder = append(groupOrder, key)
		}
		grouped[key] = append(grouped[key], table)
	}

	selected := make([]tableInfo, 0, len(tables))
	selectedPerGroup := make(map[string]int64)
	for {
		progress := false
		for _, key := range groupOrder {
			if maxResults > 0 && len(selected) >= maxResults {
				return selected, len(selected) < len(tables)
			}
			if cfg.MaxTablesPerGroup > 0 && selectedPerGroup[key] >= cfg.MaxTablesPerGroup {
				continue
			}

			queue := grouped[key]
			if len(queue) == 0 {
				continue
			}

			selected = append(selected, queue[0])
			grouped[key] = queue[1:]
			selectedPerGroup[key]++
			progress = true
		}
		if !progress {
			break
		}
	}

	return selected, len(selected) < len(tables)
}
