package gc

import (
	"context"
	"time"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	gcActivityName = "GCActivity"
)

// ApplicationName is the task list for this sample
const ApplicationName = "GCWorkGroup"

type CandenceGCProcessor struct {
	numWorkers int
	h          *common.SampleHelper
}

var _ GCProcessor = &CandenceGCProcessor{}

func NewCandenceGCProcessor(numWorkers int, h *common.SampleHelper) *CandenceGCProcessor {
	return &CandenceGCProcessor{
		numWorkers: numWorkers,
		h:          h,
	}
}

func (n *CandenceGCProcessor) Start() error {
	workflowOptions := client.StartWorkflowOptions{
		ID:                              "gc_" + uuid.New().String(),
		TaskList:                        ApplicationName,
		ExecutionStartToCloseTimeout:    time.Minute,
		DecisionTaskStartToCloseTimeout: time.Minute,
	}
	n.h.RegisterWorkflow(GCWorkflow)
	n.h.RegisterActivityWithAlias(GCActivity, gcActivityName)
	n.h.StartWorkflow(workflowOptions, GCWorkflow)
	return nil
}

func (n *CandenceGCProcessor) Stop() error {
	return nil
}

func (n *CandenceGCProcessor) Process(ctx context.Context) {
	// TODO: implement this
}

// gcWorkflow workflow decider
func GCWorkflow(ctx workflow.Context, store GCStateStore, scheduler Scheduler, numWorkers int) (err error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 2, // such a short timeout to make sample fail over very fast
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:          time.Second,
			BackoffCoefficient:       2.0,
			MaximumInterval:          time.Minute,
			ExpirationInterval:       time.Minute * 10,
			NonRetriableErrorReasons: []string{"bad-error"},
		},
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	originalCtx := context.Background()
	states, err := store.GetGCState(originalCtx)
	if err != nil {
		return err
	}
	job := generateGCJob(states, numWorkers)
	tasks, err := scheduler.Schedule(originalCtx, job)
	if err != nil {
		return err
	}
	resultChannel := workflow.GetSignalChannel(ctx, "resultChannel")
	for _, task := range tasks {
		workflow.Go(ctx, func(ctx workflow.Context) {
			var result TaskResult
			err := workflow.ExecuteActivity(ctx, gcActivityName, task).Get(ctx, &result)
			if err == nil {
				resultChannel.Send(ctx, result)
			} else {
				resultChannel.Send(ctx, err)
			}
		})
	}

	for i := 0; i < len(tasks); i++ {
		var result interface{}
		resultChannel.Receive(ctx, &result)
		if err, ok := result.(error); ok {
			workflow.GetLogger(ctx).Error("GCActivity failed.", zap.Error(err))
		} else {
			workflow.GetLogger(ctx).Info("GCActivity succeed.", zap.Any("result", result))
		}
	}
	return err
}

// GCActivity is the activity implementation.
func GCActivity(ctx context.Context, task Task) (TaskResult, error) {
	return task.Run(ctx)
}

func generateGCJob(states []GCState, parallelism int) GCJob {
	return GCJob{
		ID:          "cadence_gc_job",
		States:      states,
		parallelism: parallelism, // TODO: how to set this value?
	}
}
