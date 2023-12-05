package gc

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type GCStateStore interface {
	GetGCState(ctx context.Context) ([]GCState, error)
	AddGCState(ctx context.Context, state GCState) error
	CheckpointState(ctx context.Context, states []GCState) error
	UpdateSegment(ctx context.Context, segmentID string, status SegmentStatus) error
}

var _ GCStateStore = (*MemoryGCStateStore)(nil)

type MemoryGCStateStore struct {
	mu     sync.RWMutex
	states map[string]*GCState
}

func NewMemoryGCStateStore() *MemoryGCStateStore {
	return &MemoryGCStateStore{
		states: make(map[string]*GCState),
	}
}

func (m *MemoryGCStateStore) GetGCState(ctx context.Context) ([]GCState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var states []GCState
	for _, state := range m.states {
		newState := GCState{
			CollectionID: state.CollectionID,
			Segments:     make(map[string]SegmentState),
		}
		for segmentID, segmentState := range state.Segments {
			if *segmentState.Status == SegmentStatus_Dropping {
				newSegmentStaus := SegmentStatus_Dropping
				newState.Segments[segmentID] = SegmentState{
					SegmentID: segmentID,
					Path:      segmentState.Path,
					Status:    &newSegmentStaus,
				}
			}
		}
		states = append(states, newState)
	}
	return states, nil
}

func (m *MemoryGCStateStore) AddGCState(ctx context.Context, state GCState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.states[state.CollectionID]; !ok {
		m.states[state.CollectionID] = &state
	} else {
		for segmentID, segmentState := range state.Segments {
			m.states[state.CollectionID].Segments[segmentID] = segmentState
		}
	}
	return nil
}

func (m *MemoryGCStateStore) CheckpointState(ctx context.Context, states []GCState) error {
	for _, state := range states {
		// Update segment status to dropped.
		for _, segmentState := range state.Segments {
			// TODO: change this to batch update.
			err := m.UpdateSegment(ctx, segmentState.SegmentID, SegmentStatus_Dropped)
			if err != nil {
				log.Error("fail to update segment status", zap.String("segmentID", segmentState.SegmentID), zap.Error(err))
				return err
			}
		}
	}
	log.Info("gc state checkpointed")
	return nil
}

func (m *MemoryGCStateStore) UpdateSegment(ctx context.Context, segmentID string, status SegmentStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.states[segmentID]
	if !ok {
		return nil
	}
	segmentState := state.Segments[segmentID]
	updatedStatus := SegmentStatus_Dropped
	segmentState.Status = &updatedStatus
	return nil
}
