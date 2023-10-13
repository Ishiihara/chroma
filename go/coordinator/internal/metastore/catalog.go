package metastore

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

//go:generate mockery --name=Catalog
type Catalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts types.Timestamp) error
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, collectionID types.UniqueID) error
	UpdateCollection(ctx context.Context, collectionInfo *model.Collection, ts types.Timestamp) error
	CreateSegment(ctx context.Context, segmentInfo *model.Segment, ts types.Timestamp) error
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error)
}
