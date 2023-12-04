package gc

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TaskProcessor interface {
	Process(ctx context.Context) error
	AppendGCState(state GCState) error
}

type Task struct {
	ID         int64
	Store      GCStateStore
	State      []GCState
	ResultChan chan TaskResult
}

type TaskResult struct {
	ID     int64
	Err    error
	Result interface{}
}

func NewTask(id int64, store GCStateStore, state []GCState, resultChan chan TaskResult) *Task {
	return &Task{
		ID:         id,
		Store:      store,
		State:      state,
		ResultChan: resultChan,
	}
}

var _ TaskProcessor = &Task{}

func (t *Task) Process(ctx context.Context) error {
	for _, state := range t.State {
		for _, segmentState := range state.Segments {
			if segmentState.Status == SegmentStatus_Dropping {
				err := t.cleanUp(ctx, segmentState.SegmentID)
				if err != nil {
					log.Error("fail to clean up segment", zap.String("segmentID", segmentState.SegmentID), zap.Error(err))
					continue
				}
				err = t.Store.UpdateSegment(ctx, segmentState.SegmentID, SegmentStatus_Dropped)
				if err != nil {
					log.Error("fail to update segment status", zap.String("segmentID", segmentState.SegmentID), zap.Error(err))
					continue
				}
			}
		}
	}
	// We can potentially return the number of segments cleaned up and the error messages
	t.ResultChan <- TaskResult{
		ID:     t.ID,
		Err:    nil,
		Result: nil,
	}
	return nil
}

func (t *Task) AppendGCState(state GCState) error {
	t.State = append(t.State, state)
	return nil
}

func (t *Task) cleanUp(ctx context.Context, segmentID string) error {
	log.Info("cleaning up segment", zap.String("segmentID", segmentID))
	return nil
}
