package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

type MemoryCatalog struct {
	Collections         map[types.UniqueID]*model.Collection
	CollectionsMetadata map[types.UniqueID]*model.CollectionMetadata[model.CollectionMetadataValueType]
	Segments            map[types.UniqueID]*model.Segment
	SegmentsMetadata    map[types.UniqueID]*model.SegmentMetadata[model.SegmentMetadataValueType]
}

func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{
		Collections:         make(map[types.UniqueID]*model.Collection),
		CollectionsMetadata: make(map[types.UniqueID]*model.CollectionMetadata[model.CollectionMetadataValueType]),
		Segments:            make(map[types.UniqueID]*model.Segment),
		SegmentsMetadata:    make(map[types.UniqueID]*model.SegmentMetadata[model.SegmentMetadataValueType]),
	}
}

func (mc *MemoryCatalog) CreateCollection(ctx context.Context, collection *model.Collection, ts types.Timestamp) error {
	mc.Collections[collection.ID] = collection
	mc.CollectionsMetadata[collection.ID] = collection.Metadata
	return nil
}

func (mc *MemoryCatalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	collections := make([]*model.Collection, 0, len(mc.Collections))
	for _, collection := range mc.Collections {
		if model.FilterCollection(collection, collectionID, collectionName, collectionTopic) {
			collections = append(collections, collection)
		}
	}
	return collections, nil
}

func (mc *MemoryCatalog) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	delete(mc.Collections, collectionID)
	delete(mc.CollectionsMetadata, collectionID)
	return nil
}

func (mc *MemoryCatalog) UpdateCollection(ctx context.Context, collection *model.Collection, ts types.Timestamp) error {
	mc.Collections[collection.ID] = collection
	mc.CollectionsMetadata[collection.ID] = collection.Metadata
	return nil
}

func (mc *MemoryCatalog) CreateSegment(ctx context.Context, segment *model.Segment, ts types.Timestamp) error {
	mc.Segments[segment.ID] = segment
	mc.SegmentsMetadata[segment.ID] = segment.Metadata
	return nil
}

func (mc *MemoryCatalog) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error) {
	segments := make([]*model.Segment, 0, len(mc.Segments))
	for _, segment := range mc.Segments {
		if model.FilterSegments(segment, segmentID, segmentType, scope, topic, collectionID) {
			segments = append(segments, segment)
		}
	}
	return segments, nil
}
