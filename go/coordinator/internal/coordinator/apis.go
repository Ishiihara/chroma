package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/model"
)

func (s *Coordinator) CreateCollection(ctx context.Context, collection *model.Collection) error {
	// TODO: check if collection exists

	// Verifications on the metadata
	err := s.meta.AddCollection(ctx, collection)
	if err != nil {
		return err
	}
	return nil
}
