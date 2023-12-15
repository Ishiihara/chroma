package gc

import (
	"context"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Task struct {
	ID     string
	States []CollectionState
}

type TaskResult struct {
	ID     string
	States []CollectionState
	Err    error
}

func NewTask(states []CollectionState) Task {
	return Task{
		ID:     uuid.New().String(),
		States: states,
	}
}

func (t *Task) Run(ctx context.Context) (TaskResult, error) {
	for _, state := range t.States {
		for _, segmentState := range state.Segments {
			if *segmentState.Status == SegmentStatus_Dropping {
				err := t.cleanUp(ctx, segmentState.SegmentID)
				if err != nil {
					log.Error("fail to clean up segment", zap.String("segmentID", segmentState.SegmentID), zap.Error(err))
					continue
				}
				*segmentState.Status = SegmentStatus_Dropped
			}
		}
	}
	result := TaskResult{
		ID:     t.ID,
		States: t.States,
		Err:    nil,
	}
	log.Info("gc task finished", zap.Any("taskID", result))
	return result, nil
}

func (t *Task) cleanUp(ctx context.Context, segmentID string) error {
	// TODO: add the actual logic to clean up the segment
	log.Info("cleaning up segment", zap.String("segmentID", segmentID))
	return nil
}
