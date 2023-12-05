package gc

import (
	"context"

	"github.com/pingcap/log"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type GCJob struct {
	ID          string
	States      []GCState
	parallelism int
}

type Scheduler interface {
	Schedule(ctx context.Context, job GCJob) ([]Task, error)
}

type SchedulerImpl struct {
}

func NewSchedulerImpl() *SchedulerImpl {
	return &SchedulerImpl{}
}

var _ Scheduler = &SchedulerImpl{}

func (s *SchedulerImpl) Schedule(ctx context.Context, job GCJob) ([]Task, error) {
	// Create tasks, the number of tasks should be equal to the number of workers
	hash := murmur3.New64()
	tasks := make([]Task, job.parallelism)
	for i := 0; i < job.parallelism; i++ {
		tasks[i] = NewGCTask()
	}
	for _, state := range job.States {
		collectionID := state.CollectionID
		// assign collections to tasks, alternatively we can assign based on segments
		hashValue, err := hash.Write([]byte(collectionID))
		if err != nil {
			return nil, err
		}
		taskIndex := hashValue % job.parallelism
		log.Info("assign collection to task", zap.Any("state", state), zap.Int("taskIndex", taskIndex))
		tasks[taskIndex].AppendGCState(state)
	}
	return tasks, nil
}
