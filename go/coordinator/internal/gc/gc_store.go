package gc

import (
	"context"
	"sync"
)

type JobStateStore interface {
	AddJob(ctx context.Context, job Job) error
	UpdateState(ctx context.Context, jobID string, taskResult TaskResult) error
}

var _ JobStateStore = (*MemoryJobStateStore)(nil)

type MemoryJobStateStore struct {
	mu         sync.RWMutex
	jobState   map[string]string
	taskStates map[string][]CollectionState
}

func NewMemoryJobStateStore() *MemoryJobStateStore {
	return &MemoryJobStateStore{
		jobState:   make(map[string]string),
		taskStates: make(map[string][]CollectionState),
	}
}

func (m *MemoryJobStateStore) AddJob(ctx context.Context, job Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobState[job.ID] = "pending"
	for _, task := range job.tasks {
		m.taskStates[task.ID] = task.States
	}
	return nil
}

func (m *MemoryJobStateStore) UpdateState(ctx context.Context, jobID string, taskResult TaskResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	taskID := taskResult.ID
	m.taskStates[taskID] = taskResult.States
	return nil
}
