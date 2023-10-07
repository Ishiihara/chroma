package metastore

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

//go:generate mockery --name=Catalog
type Catalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts types.Timestamp) error
	ListCollections(ctx context.Context, ts types.Timestamp) ([]*model.Collection, error)
}
