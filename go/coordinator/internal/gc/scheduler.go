package gc

import (
	"context"

	"github.com/twmb/murmur3"
)

type Job struct {
	ID          string
	States      []CollectionState
	tasks       []Task
	parallelism int
}

type Scheduler interface {
	Schedule(ctx context.Context, job *Job, partitioner Partinioner) ([]Task, error)
}

type SchedulerImpl struct {
}

func NewSchedulerImpl() *SchedulerImpl {
	return &SchedulerImpl{}
}

type Partinioner interface {
	Partition(ctx context.Context, collectionStates []CollectionState, numPartitions int) ([][]CollectionState, error)
}

type HashPartitioner struct {
}

func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{}
}

func (h *HashPartitioner) Partition(ctx context.Context, collectionStates []CollectionState, numPartitions int) ([][]CollectionState, error) {
	hash := murmur3.New64()
	taskStates := make([][]CollectionState, numPartitions)
	for _, state := range collectionStates {
		collectionID := state.CollectionID
		// assign collections to tasks, alternatively we can assign based on segments
		hashValue, err := hash.Write([]byte(collectionID))
		if err != nil {
			return nil, err
		}
		taskIndex := hashValue % numPartitions
		taskStates[taskIndex] = append(taskStates[taskIndex], state)
	}
	return taskStates, nil
}

var _ Scheduler = &SchedulerImpl{}

func (s *SchedulerImpl) Schedule(ctx context.Context, job *Job, partitioner Partinioner) ([]Task, error) {
	CollectionStates := job.States
	taskStates, err := partitioner.Partition(ctx, CollectionStates, job.parallelism)
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, 0)
	for _, taskState := range taskStates {
		tasks = append(tasks, NewTask(taskState))
	}
	return tasks, nil
}
