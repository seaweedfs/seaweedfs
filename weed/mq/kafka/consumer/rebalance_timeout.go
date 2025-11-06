package consumer

import (
	"time"
)

// RebalanceTimeoutManager handles rebalance timeout logic and member eviction
type RebalanceTimeoutManager struct {
	coordinator *GroupCoordinator
}

// NewRebalanceTimeoutManager creates a new rebalance timeout manager
func NewRebalanceTimeoutManager(coordinator *GroupCoordinator) *RebalanceTimeoutManager {
	return &RebalanceTimeoutManager{
		coordinator: coordinator,
	}
}

// CheckRebalanceTimeouts checks for members that have exceeded rebalance timeouts
func (rtm *RebalanceTimeoutManager) CheckRebalanceTimeouts() {
	now := time.Now()
	rtm.coordinator.groupsMu.RLock()
	defer rtm.coordinator.groupsMu.RUnlock()

	for _, group := range rtm.coordinator.groups {
		group.Mu.Lock()

		// Only check timeouts for groups in rebalancing states
		if group.State == GroupStatePreparingRebalance || group.State == GroupStateCompletingRebalance {
			rtm.checkGroupRebalanceTimeout(group, now)
		}

		group.Mu.Unlock()
	}
}

// checkGroupRebalanceTimeout checks and handles rebalance timeout for a specific group
func (rtm *RebalanceTimeoutManager) checkGroupRebalanceTimeout(group *ConsumerGroup, now time.Time) {
	expiredMembers := make([]string, 0)

	for memberID, member := range group.Members {
		// Check if member has exceeded its rebalance timeout
		rebalanceTimeout := time.Duration(member.RebalanceTimeout) * time.Millisecond
		if rebalanceTimeout == 0 {
			// Use default rebalance timeout if not specified
			rebalanceTimeout = time.Duration(rtm.coordinator.rebalanceTimeoutMs) * time.Millisecond
		}

		// For members in pending state during rebalance, check against join time
		if member.State == MemberStatePending {
			if now.Sub(member.JoinedAt) > rebalanceTimeout {
				expiredMembers = append(expiredMembers, memberID)
			}
		}

		// Also check session timeout as a fallback
		sessionTimeout := time.Duration(member.SessionTimeout) * time.Millisecond
		if now.Sub(member.LastHeartbeat) > sessionTimeout {
			expiredMembers = append(expiredMembers, memberID)
		}
	}

	// Remove expired members and trigger rebalance if necessary
	if len(expiredMembers) > 0 {
		rtm.evictExpiredMembers(group, expiredMembers)
	}
}

// evictExpiredMembers removes expired members and updates group state
func (rtm *RebalanceTimeoutManager) evictExpiredMembers(group *ConsumerGroup, expiredMembers []string) {
	for _, memberID := range expiredMembers {
		delete(group.Members, memberID)

		// If the leader was evicted, clear leader
		if group.Leader == memberID {
			group.Leader = ""
		}
	}

	// Update group state based on remaining members
	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
		group.Generation++
		group.Leader = ""
	} else {
		// If we were in the middle of rebalancing, restart the process
		if group.State == GroupStatePreparingRebalance || group.State == GroupStateCompletingRebalance {
			// Select new leader if needed
			if group.Leader == "" {
				for memberID := range group.Members {
					group.Leader = memberID
					break
				}
			}

			// Reset to preparing rebalance to restart the process
			group.State = GroupStatePreparingRebalance
			group.Generation++

			// Mark remaining members as pending
			for _, member := range group.Members {
				member.State = MemberStatePending
			}
		}
	}

	group.LastActivity = time.Now()
}

// IsRebalanceStuck checks if a group has been stuck in rebalancing for too long
func (rtm *RebalanceTimeoutManager) IsRebalanceStuck(group *ConsumerGroup, maxRebalanceDuration time.Duration) bool {
	if group.State != GroupStatePreparingRebalance && group.State != GroupStateCompletingRebalance {
		return false
	}

	return time.Since(group.LastActivity) > maxRebalanceDuration
}

// ForceCompleteRebalance forces completion of a stuck rebalance
func (rtm *RebalanceTimeoutManager) ForceCompleteRebalance(group *ConsumerGroup) {
	group.Mu.Lock()
	defer group.Mu.Unlock()

	// If stuck in preparing rebalance, move to completing
	if group.State == GroupStatePreparingRebalance {
		group.State = GroupStateCompletingRebalance
		group.LastActivity = time.Now()
		return
	}

	// If stuck in completing rebalance, force to stable
	if group.State == GroupStateCompletingRebalance {
		group.State = GroupStateStable
		for _, member := range group.Members {
			member.State = MemberStateStable
		}
		group.LastActivity = time.Now()
		return
	}
}

// GetRebalanceStatus returns the current rebalance status for a group
func (rtm *RebalanceTimeoutManager) GetRebalanceStatus(groupID string) *RebalanceStatus {
	group := rtm.coordinator.GetGroup(groupID)
	if group == nil {
		return nil
	}

	group.Mu.RLock()
	defer group.Mu.RUnlock()

	status := &RebalanceStatus{
		GroupID:           groupID,
		State:             group.State,
		Generation:        group.Generation,
		MemberCount:       len(group.Members),
		Leader:            group.Leader,
		LastActivity:      group.LastActivity,
		IsRebalancing:     group.State == GroupStatePreparingRebalance || group.State == GroupStateCompletingRebalance,
		RebalanceDuration: time.Since(group.LastActivity),
	}

	// Calculate member timeout status
	now := time.Now()
	for memberID, member := range group.Members {
		memberStatus := MemberTimeoutStatus{
			MemberID:         memberID,
			State:            member.State,
			LastHeartbeat:    member.LastHeartbeat,
			JoinedAt:         member.JoinedAt,
			SessionTimeout:   time.Duration(member.SessionTimeout) * time.Millisecond,
			RebalanceTimeout: time.Duration(member.RebalanceTimeout) * time.Millisecond,
		}

		// Calculate time until session timeout
		sessionTimeRemaining := memberStatus.SessionTimeout - now.Sub(member.LastHeartbeat)
		if sessionTimeRemaining < 0 {
			sessionTimeRemaining = 0
		}
		memberStatus.SessionTimeRemaining = sessionTimeRemaining

		// Calculate time until rebalance timeout
		rebalanceTimeRemaining := memberStatus.RebalanceTimeout - now.Sub(member.JoinedAt)
		if rebalanceTimeRemaining < 0 {
			rebalanceTimeRemaining = 0
		}
		memberStatus.RebalanceTimeRemaining = rebalanceTimeRemaining

		status.Members = append(status.Members, memberStatus)
	}

	return status
}

// RebalanceStatus represents the current status of a group's rebalance
type RebalanceStatus struct {
	GroupID           string                `json:"group_id"`
	State             GroupState            `json:"state"`
	Generation        int32                 `json:"generation"`
	MemberCount       int                   `json:"member_count"`
	Leader            string                `json:"leader"`
	LastActivity      time.Time             `json:"last_activity"`
	IsRebalancing     bool                  `json:"is_rebalancing"`
	RebalanceDuration time.Duration         `json:"rebalance_duration"`
	Members           []MemberTimeoutStatus `json:"members"`
}

// MemberTimeoutStatus represents timeout status for a group member
type MemberTimeoutStatus struct {
	MemberID               string        `json:"member_id"`
	State                  MemberState   `json:"state"`
	LastHeartbeat          time.Time     `json:"last_heartbeat"`
	JoinedAt               time.Time     `json:"joined_at"`
	SessionTimeout         time.Duration `json:"session_timeout"`
	RebalanceTimeout       time.Duration `json:"rebalance_timeout"`
	SessionTimeRemaining   time.Duration `json:"session_time_remaining"`
	RebalanceTimeRemaining time.Duration `json:"rebalance_time_remaining"`
}
