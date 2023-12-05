package gc

import (
	"context"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TaskProcessor interface {
	Process(ctx context.Context) error
}

type Task interface {
	Run(ctx context.Context) (TaskResult, error)
	AppendGCState(state GCState) error
}

type GCTask struct {
	ID    string
	State []GCState
}

type TaskResult struct {
	ID     string
	States []GCState
	Err    error
}

var _ Task = &GCTask{}

func NewGCTask() *GCTask {
	return &GCTask{
		ID: uuid.New().String(),
	}
}

func (t *GCTask) AppendGCState(state GCState) error {
	t.State = append(t.State, state)
	return nil
}

func (t *GCTask) Run(ctx context.Context) (TaskResult, error) {
	for _, state := range t.State {
		for _, segmentState := range state.Segments {
			if *segmentState.Status == SegmentStatus_Dropping {
				err := t.cleanUp(ctx, segmentState.SegmentID)
				if err != nil {
					log.Error("fail to clean up segment", zap.String("segmentID", segmentState.SegmentID), zap.Error(err))
					continue
				}
				*segmentState.Status = SegmentStatus_Dropped
				if err != nil {
					log.Error("fail to update segment status", zap.String("segmentID", segmentState.SegmentID), zap.Error(err))
					continue
				}
			}
		}
	}
	result := TaskResult{
		ID:     t.ID,
		States: t.State,
	}
	log.Info("gc task finished", zap.Any("taskID", result))
	return result, nil
}

func (t *GCTask) cleanUp(ctx context.Context, segmentID string) error {
	log.Info("cleaning up segment", zap.String("segmentID", segmentID))
	return nil
}
