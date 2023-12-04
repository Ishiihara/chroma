package gc

import (
	"context"
	"sync"
)

type GCStateStore interface {
	GetGCState(ctx context.Context) ([]GCState, error)
	UpdateSegment(ctx context.Context, segmentID string, status SegmentStatus) error
}

type MemoryGCStateStore struct {
	mu     sync.RWMutex
	states map[string]GCState
}

func NewMemoryGCStateStore() *MemoryGCStateStore {
	return &MemoryGCStateStore{
		states: make(map[string]GCState),
	}
}

func (m *MemoryGCStateStore) GetGCState(ctx context.Context) ([]GCState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var states []GCState
	for _, state := range m.states {
		states = append(states, state)
	}
	return states, nil
}

func (m *MemoryGCStateStore) UpdateSegment(ctx context.Context, segmentID string, status SegmentStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.states[segmentID]
	if !ok {
		return nil
	}
	segmentState := state.Segments[segmentID]
	segmentState.Status = status
	return nil
}
