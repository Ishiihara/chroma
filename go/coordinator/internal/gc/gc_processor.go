package gc

import (
	"context"
	"time"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type GCProcessor interface {
	common.Component
	Process(ctx context.Context)
}

type SimpleGCProcessor struct {
	inputStore  InputStore
	store       JobStateStore
	gcInterval  time.Duration
	scheduler   Scheduler
	jobRunner   JobRunner
	done        chan bool
	parallelism int
}

var _ GCProcessor = &SimpleGCProcessor{}

func NewSimpleGCProcessor(inputStore InputStore, outputStore OutputStore, store JobStateStore, gcInterval time.Duration, parallelism int) *SimpleGCProcessor {
	processor := &SimpleGCProcessor{
		inputStore:  inputStore,
		store:       store,
		gcInterval:  gcInterval,
		scheduler:   NewSchedulerImpl(),
		jobRunner:   NewSimpleJobRunner(outputStore, store, NewSimpleTaskRunner(), make(chan TaskResult)),
		done:        make(chan bool),
		parallelism: parallelism,
	}
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
			gcJob := n.generateGCJob(ctx)
			n.jobRunner.Run(ctx, *gcJob)
		}
	}
}

func (n *SimpleGCProcessor) generateGCJob(ctx context.Context) *Job {
	inputs, err := n.inputStore.Input(ctx)
	if err != nil {
		log.Error("fail to get gc state", zap.Error(err))
		return nil
	}

	gcJob := &Job{
		ID:          "gc_job",
		States:      inputs,
		parallelism: n.parallelism,
	}
	partitioner := &HashPartitioner{}

	tasks, err := n.scheduler.Schedule(ctx, gcJob, partitioner)
	if err != nil {
		log.Error("fail to schedule gc job", zap.Error(err))
	}
	gcJob.tasks = tasks
	return gcJob
}
