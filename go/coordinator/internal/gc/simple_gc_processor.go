package gc

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SimpleGCProcessor struct {
	numWorkers int
	store      GCStateStore
	gcInterval time.Duration
	scheduler  Scheduler
	taskRunner TaskRunner
	done       chan bool
}

var _ GCProcessor = &SimpleGCProcessor{}

func NewSimpleGCProcessor(numWorkers int, store GCStateStore, gcInterval time.Duration) *SimpleGCProcessor {
	processor := &SimpleGCProcessor{
		numWorkers: numWorkers,
		store:      store,
		gcInterval: gcInterval,
		done:       make(chan bool),
	}
	processor.scheduler = NewSchedulerImpl()
	processor.taskRunner = NewSimpleTaskRunner(numWorkers)
	return processor
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
			resultChan := make(chan TaskResult, 100)
			gcJob := n.generateGCJob(ctx)
			tasks, err := n.scheduler.Schedule(ctx, gcJob)
			if err != nil {
				log.Error("fail to schedule gc job", zap.Error(err))
				continue
			}
			for _, task := range tasks {
				err := n.taskRunner.Execute(ctx, task, resultChan)
				if err != nil {
					log.Error("fail to execute gc task", zap.Error(err))
					continue
				}
			}
			numPendingTasks := len(tasks)
			for result := range resultChan {
				if result.Err != nil {
					log.Error("fail to execute gc task", zap.Error(result.Err))
					continue
				}
				log.Info("gc task finished", zap.String("taskID", result.ID))
				err := n.store.CheckpointState(ctx, result.States)
				if err != nil {
					log.Error("fail to checkpoint gc task", zap.Error(err))
					continue
				}
				log.Info("gc task checkpointed", zap.String("taskID", result.ID))
				numPendingTasks--
				if numPendingTasks == 0 {
					break
				}
			}
		}
	}
}

func (n *SimpleGCProcessor) generateGCJob(ctx context.Context) GCJob {
	states, err := n.store.GetGCState(ctx)
	if err != nil {
		log.Error("fail to get gc state", zap.Error(err))
		return GCJob{}
	}
	log.Info("gc job generated", zap.Any("states", states))
	return GCJob{
		ID:          "gc_job",
		States:      states,
		parallelism: n.numWorkers, // TODO: how to set this value?
	}
}
