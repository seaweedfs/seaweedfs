package protocol

import (
	"sort"
	"time"
)

type DebugGroupSnapshot struct {
	GroupID          string                      `json:"group_id"`
	Exists           bool                        `json:"exists"`
	KnownGroups      []string                    `json:"known_groups,omitempty"`
	State            string                      `json:"state,omitempty"`
	Generation       int32                       `json:"generation,omitempty"`
	Protocol         string                      `json:"protocol,omitempty"`
	Leader           string                      `json:"leader,omitempty"`
	CreatedAt        time.Time                   `json:"created_at,omitempty"`
	LastActivity     time.Time                   `json:"last_activity,omitempty"`
	SubscribedTopics []string                    `json:"subscribed_topics,omitempty"`
	Members          []DebugGroupMemberSnapshot  `json:"members,omitempty"`
	OffsetCommits    []DebugOffsetCommitSnapshot `json:"offset_commits,omitempty"`
}

type DebugGroupMemberSnapshot struct {
	MemberID           string    `json:"member_id"`
	ClientID           string    `json:"client_id,omitempty"`
	ConnectionID       string    `json:"connection_id,omitempty"`
	ClientHost         string    `json:"client_host,omitempty"`
	State              string    `json:"state,omitempty"`
	SessionTimeoutMs   int32     `json:"session_timeout_ms,omitempty"`
	RebalanceTimeoutMs int32     `json:"rebalance_timeout_ms,omitempty"`
	JoinedAt           time.Time `json:"joined_at,omitempty"`
	LastHeartbeat      time.Time `json:"last_heartbeat,omitempty"`
	LastHeartbeatAgeMs int64     `json:"last_heartbeat_age_ms,omitempty"`
	Subscription       []string  `json:"subscription,omitempty"`
	AssignedPartitions []string  `json:"assigned_partitions,omitempty"`
	GroupInstanceID    string    `json:"group_instance_id,omitempty"`
}

type DebugOffsetCommitSnapshot struct {
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	Metadata    string    `json:"metadata,omitempty"`
	Timestamp   time.Time `json:"timestamp,omitempty"`
	CommitAgeMs int64     `json:"commit_age_ms,omitempty"`
}

func (h *Handler) DebugGroupSnapshot(groupID string) *DebugGroupSnapshot {
	snapshot := &DebugGroupSnapshot{
		GroupID:     groupID,
		KnownGroups: h.debugKnownGroups(),
	}
	if h == nil || h.groupCoordinator == nil {
		return snapshot
	}

	group := h.groupCoordinator.GetGroup(groupID)
	if group == nil {
		return snapshot
	}

	now := time.Now()

	group.Mu.RLock()
	defer group.Mu.RUnlock()

	snapshot.Exists = true
	snapshot.State = group.State.String()
	snapshot.Generation = group.Generation
	snapshot.Protocol = group.Protocol
	snapshot.Leader = group.Leader
	snapshot.CreatedAt = group.CreatedAt
	snapshot.LastActivity = group.LastActivity

	if len(group.SubscribedTopics) > 0 {
		topics := make([]string, 0, len(group.SubscribedTopics))
		for topic := range group.SubscribedTopics {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		snapshot.SubscribedTopics = topics
	}

	memberIDs := make([]string, 0, len(group.Members))
	for memberID := range group.Members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	snapshot.Members = make([]DebugGroupMemberSnapshot, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		member := group.Members[memberID]
		if member == nil {
			continue
		}

		subscription := append([]string(nil), member.Subscription...)
		sort.Strings(subscription)

		assignments := make([]string, 0, len(member.Assignment))
		for _, assignment := range member.Assignment {
			assignments = append(assignments, assignment.Topic+":"+itoa32(assignment.Partition))
		}
		sort.Strings(assignments)

		groupInstanceID := ""
		if member.GroupInstanceID != nil {
			groupInstanceID = *member.GroupInstanceID
		}

		snapshot.Members = append(snapshot.Members, DebugGroupMemberSnapshot{
			MemberID:           member.ID,
			ClientID:           member.ClientID,
			ConnectionID:       member.ConnectionID,
			ClientHost:         member.ClientHost,
			State:              member.State.String(),
			SessionTimeoutMs:   member.SessionTimeout,
			RebalanceTimeoutMs: member.RebalanceTimeout,
			JoinedAt:           member.JoinedAt,
			LastHeartbeat:      member.LastHeartbeat,
			LastHeartbeatAgeMs: ageMillis(now, member.LastHeartbeat),
			Subscription:       subscription,
			AssignedPartitions: assignments,
			GroupInstanceID:    groupInstanceID,
		})
	}

	if len(group.OffsetCommits) > 0 {
		topics := make([]string, 0, len(group.OffsetCommits))
		for topic := range group.OffsetCommits {
			topics = append(topics, topic)
		}
		sort.Strings(topics)

		offsets := make([]DebugOffsetCommitSnapshot, 0)
		for _, topic := range topics {
			partitions := make([]int32, 0, len(group.OffsetCommits[topic]))
			for partition := range group.OffsetCommits[topic] {
				partitions = append(partitions, partition)
			}
			sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
			for _, partition := range partitions {
				offsetCommit := group.OffsetCommits[topic][partition]
				offsets = append(offsets, DebugOffsetCommitSnapshot{
					Topic:       topic,
					Partition:   partition,
					Offset:      offsetCommit.Offset,
					Metadata:    offsetCommit.Metadata,
					Timestamp:   offsetCommit.Timestamp,
					CommitAgeMs: ageMillis(now, offsetCommit.Timestamp),
				})
			}
		}
		snapshot.OffsetCommits = offsets
	}

	return snapshot
}

func (h *Handler) debugKnownGroups() []string {
	if h == nil || h.groupCoordinator == nil {
		return nil
	}

	groupIDs := h.groupCoordinator.ListGroups()
	sort.Strings(groupIDs)
	return groupIDs
}

func ageMillis(now, then time.Time) int64 {
	if then.IsZero() {
		return 0
	}
	return now.Sub(then).Milliseconds()
}

func itoa32(value int32) string {
	if value == 0 {
		return "0"
	}

	negative := value < 0
	if negative {
		value = -value
	}

	var digits [12]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + (value % 10))
		value /= 10
	}
	if negative {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
