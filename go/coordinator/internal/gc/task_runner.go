package gc

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TaskRunner interface {
	Execute(ctx context.Context, task Task, resultChan chan TaskResult) error
}

type SimpleTaskRunner struct {
}

func NewSimpleTaskRunner() *SimpleTaskRunner {
	return &SimpleTaskRunner{}
}

var _ TaskRunner = &SimpleTaskRunner{}

func (s *SimpleTaskRunner) Execute(ctx context.Context, task Task, resultChan chan TaskResult) error {
	go func() {
		log.Info("start to execute task", zap.Any("task", task))
		result, err := task.Run(ctx)
		if err != nil {
			result.Err = err
		}
		resultChan <- result
	}()
	return nil
}
