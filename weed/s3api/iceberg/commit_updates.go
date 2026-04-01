package iceberg

import (
	"encoding/json"
	"errors"

	"github.com/apache/iceberg-go/table"
)

type statisticsUpdate struct {
	set    *table.StatisticsFile
	remove *int64
}

var ErrIncompleteSetStatistics = errors.New("set-statistics requires snapshot-id, statistics-path, file-size-in-bytes, and file-footer-size-in-bytes")

type commitAction struct {
	Action string `json:"action"`
}

type setStatisticsUpdate struct {
	Action                string                `json:"action"`
	SnapshotID            *int64                `json:"snapshot-id,omitempty"`
	StatisticsPath        string                `json:"statistics-path,omitempty"`
	FileSizeInBytes       *int64                `json:"file-size-in-bytes,omitempty"`
	FileFooterSizeInBytes *int64                `json:"file-footer-size-in-bytes,omitempty"`
	KeyMetadata           *string               `json:"key-metadata,omitempty"`
	BlobMetadata          []table.BlobMetadata  `json:"blob-metadata,omitempty"`
	Statistics            *table.StatisticsFile `json:"statistics,omitempty"`
}

func (u *setStatisticsUpdate) asStatisticsFile() (*table.StatisticsFile, error) {
	if u.Statistics != nil {
		if u.Statistics.BlobMetadata == nil {
			u.Statistics.BlobMetadata = []table.BlobMetadata{}
		}
		return u.Statistics, nil
	}
	if u.SnapshotID == nil || u.StatisticsPath == "" || u.FileSizeInBytes == nil || u.FileFooterSizeInBytes == nil {
		return nil, ErrIncompleteSetStatistics
	}

	stats := &table.StatisticsFile{
		SnapshotID:            *u.SnapshotID,
		StatisticsPath:        u.StatisticsPath,
		FileSizeInBytes:       *u.FileSizeInBytes,
		FileFooterSizeInBytes: *u.FileFooterSizeInBytes,
		KeyMetadata:           u.KeyMetadata,
		BlobMetadata:          u.BlobMetadata,
	}
	if stats.BlobMetadata == nil {
		stats.BlobMetadata = []table.BlobMetadata{}
	}
	return stats, nil
}

type removeStatisticsUpdate struct {
	Action     string `json:"action"`
	SnapshotID int64  `json:"snapshot-id"`
}

func parseCommitUpdates(rawUpdates []json.RawMessage) (table.Updates, []statisticsUpdate, error) {
	filtered := make([]json.RawMessage, 0, len(rawUpdates))
	statisticsUpdates := make([]statisticsUpdate, 0)

	for _, raw := range rawUpdates {
		var action commitAction
		if err := json.Unmarshal(raw, &action); err != nil {
			return nil, nil, err
		}

		switch action.Action {
		case "set-statistics":
			var setUpdate setStatisticsUpdate
			if err := json.Unmarshal(raw, &setUpdate); err != nil {
				return nil, nil, err
			}
			stats, err := setUpdate.asStatisticsFile()
			if err != nil {
				return nil, nil, err
			}
			statisticsUpdates = append(statisticsUpdates, statisticsUpdate{set: stats})
		case "remove-statistics":
			var removeUpdate removeStatisticsUpdate
			if err := json.Unmarshal(raw, &removeUpdate); err != nil {
				return nil, nil, err
			}
			snapshotID := removeUpdate.SnapshotID
			statisticsUpdates = append(statisticsUpdates, statisticsUpdate{remove: &snapshotID})
		default:
			filtered = append(filtered, raw)
		}
	}

	if len(filtered) == 0 {
		return nil, statisticsUpdates, nil
	}

	data, err := json.Marshal(filtered)
	if err != nil {
		return nil, nil, err
	}
	var updates table.Updates
	if err := json.Unmarshal(data, &updates); err != nil {
		return nil, nil, err
	}

	return updates, statisticsUpdates, nil
}

func applyStatisticsUpdates(metadataBytes []byte, updates []statisticsUpdate) ([]byte, error) {
	if len(updates) == 0 {
		return metadataBytes, nil
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, err
	}

	var statistics []table.StatisticsFile
	if rawStatistics, ok := metadata["statistics"]; ok && len(rawStatistics) > 0 {
		if err := json.Unmarshal(rawStatistics, &statistics); err != nil {
			return nil, err
		}
	}

	statisticsBySnapshot := make(map[int64]table.StatisticsFile, len(statistics))
	orderedSnapshotIDs := make([]int64, 0, len(statistics))
	inOrder := make(map[int64]bool, len(statistics))
	for _, stat := range statistics {
		statisticsBySnapshot[stat.SnapshotID] = stat
		if !inOrder[stat.SnapshotID] {
			orderedSnapshotIDs = append(orderedSnapshotIDs, stat.SnapshotID)
			inOrder[stat.SnapshotID] = true
		}
	}

	for _, update := range updates {
		if update.set != nil {
			statisticsBySnapshot[update.set.SnapshotID] = *update.set
			if !inOrder[update.set.SnapshotID] {
				orderedSnapshotIDs = append(orderedSnapshotIDs, update.set.SnapshotID)
				inOrder[update.set.SnapshotID] = true
			}
			continue
		}
		if update.remove != nil {
			delete(statisticsBySnapshot, *update.remove)
		}
	}

	statistics = make([]table.StatisticsFile, 0, len(statisticsBySnapshot))
	for _, snapshotID := range orderedSnapshotIDs {
		stat, ok := statisticsBySnapshot[snapshotID]
		if !ok {
			continue
		}
		statistics = append(statistics, stat)
	}

	if len(statistics) == 0 {
		delete(metadata, "statistics")
	} else {
		data, err := json.Marshal(statistics)
		if err != nil {
			return nil, err
		}
		metadata["statistics"] = data
	}

	return json.Marshal(metadata)
}
