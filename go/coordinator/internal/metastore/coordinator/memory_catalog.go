package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

type MemoryCatalog struct {
	Collections         map[types.UniqueID]*model.Collection
	CollectionsMetadata map[types.UniqueID]*model.CollectionMetadata[model.MetadataValueType]
}

func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{
		Collections:         make(map[types.UniqueID]*model.Collection),
		CollectionsMetadata: make(map[types.UniqueID]*model.CollectionMetadata[model.MetadataValueType]),
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
		if model.FilterCondition(collection, collectionID, collectionName, collectionTopic) {
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
