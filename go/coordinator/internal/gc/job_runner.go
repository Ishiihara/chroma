package gc

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type JobRunner interface {
	Run(ctx context.Context, job Job) error
}

type SimpleJobRunner struct {
	outputStore OutputStore
	store       JobStateStore
	taskRunner  TaskRunner
	resultChan  chan TaskResult
}

var _ JobRunner = &SimpleJobRunner{}

func NewSimpleJobRunner(outputStore OutputStore, store JobStateStore, taskRunner TaskRunner, resultChan chan TaskResult) *SimpleJobRunner {
	return &SimpleJobRunner{
		outputStore: outputStore,
		store:       store,
		taskRunner:  taskRunner,
		resultChan:  resultChan,
	}
}

func (s *SimpleJobRunner) Run(ctx context.Context, job Job) error {
	log.Info("running job", zap.Any("job", job))
	s.store.AddJob(ctx, job)
	tasks := job.tasks
	for _, task := range tasks {
		s.taskRunner.Execute(ctx, task, s.resultChan)
	}
	for i := 0; i < len(tasks); i++ {
		result := <-s.resultChan
		log.Info("task finished", zap.Any("result", result))
		s.store.UpdateState(ctx, result.ID, result)
		s.outputStore.Output(ctx, result)
	}
	return nil
}
