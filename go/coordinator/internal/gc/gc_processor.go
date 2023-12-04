package gc

import (
	"context"
	"time"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/pingcap/log"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type GCProcessor interface {
	common.Component
	Process(ctx context.Context)
}

type SimpleGCProcessor struct {
	numWorkers int
	numEntries int
	store      GCStateStore
	gcInterval time.Duration
	done       chan bool
}

var _ GCProcessor = &SimpleGCProcessor{}

func NewSimpleGCProcessor(numWorkers int, numEntries int, store GCStateStore, gcInterval time.Duration) *SimpleGCProcessor {
	return &SimpleGCProcessor{
		numWorkers: numWorkers,
		numEntries: numEntries,
		store:      store,
		gcInterval: gcInterval,
	}
}

func (n *SimpleGCProcessor) Start() error {
	go n.Process(context.Background())
	return nil
}

func (n *SimpleGCProcessor) Stop() error {
	n.done <- true
	return nil
}

func (n *SimpleGCProcessor) Process(ctx context.Context) {
	ticker := time.NewTicker(n.gcInterval)
	for {
		select {
		case <-n.done:
			return
		case <-ticker.C:
			// GC logic goes here, single thread version
			states, err := n.store.GetGCState(ctx)
			if err != nil {
				log.Error("fail to get gc state", zap.Error(err))
				continue
			}
			resultChan := make(chan TaskResult, n.numWorkers)
			tasks, err := n.partition(ctx, states, resultChan) // We can also persist the tasks to a queue/database and let workers consume them
			if err != nil {
				log.Error("fail to get gc state", zap.Error(err))
				continue
			}
			for _, task := range tasks {
				go task.Process(ctx)
			}
			numberofProcessedTasks := n.numWorkers
			numbrofFailedTasks := 0
			for result := range resultChan {
				if result.Err != nil {
					log.Error("fail to process gc task", zap.Error(result.Err))
					numbrofFailedTasks++
				}
				numberofProcessedTasks--
				if numberofProcessedTasks == 0 {
					break
				}
			}
		}
	}
}

func (n *SimpleGCProcessor) partition(ctx context.Context, gcStates []GCState, resustChan chan TaskResult) ([]Task, error) {
	// Create tasks, the number of tasks should be equal to the number of workers
	hash := murmur3.New64()

	tasks := make([]Task, n.numWorkers)
	for i := 0; i < n.numWorkers; i++ {
		tasks[i] = *NewTask(int64(i), n.store, nil, resustChan)
	}
	for _, state := range gcStates {
		collectionID := state.CollectionID
		// assign collections to tasks, alternatively we can assign based on segments
		hashValue, err := hash.Write([]byte(collectionID))
		if err != nil {
			return nil, err
		}
		taskIndex := hashValue % n.numWorkers
		tasks[taskIndex].AppendGCState(state)
	}
	return tasks, nil
}

type GCState struct {
	CollectionID string
	Segments     map[string]SegmentState
}

type SegmentStatus int32

const (
	// SegmentStatus_Dropping indicates the segment is being dropped.
	SegmentStatus_Dropping SegmentStatus = 0
	// SegmentStatus_Dropped indicates the segment is dropped.
	SegmentStatus_Dropped SegmentStatus = 1
)

type SegmentState struct {
	SegmentID string
	Path      string
	Status    SegmentStatus
}
