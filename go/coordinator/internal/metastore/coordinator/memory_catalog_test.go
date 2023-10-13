package coordinator

import (
	"context"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

func TestMemoryCatalog(t *testing.T) {
	ctx := context.Background()
	mc := NewMemoryCatalog()

	// Test CreateCollection
	collection := &model.Collection{
		ID:    types.NewUniqueID(),
		Name:  "test-collection-name",
		Topic: "test-collection-topic",
		Metadata: &model.CollectionMetadata[model.CollectionMetadataValueType]{
			Metadata: map[string]model.CollectionMetadataValueType{
				"test-metadata-key": &model.CollectionMetadataValueStringType{Value: "test-metadata-value"},
			},
		},
	}
	if err := mc.CreateCollection(ctx, collection, types.Timestamp(0)); err != nil {
		t.Fatalf("unexpected error creating collection: %v", err)
	}
	if len(mc.Collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(mc.Collections))
	}
	if len(mc.CollectionsMetadata) != 1 {
		t.Fatalf("expected 1 collection metadata, got %d", len(mc.CollectionsMetadata))
	}
	if mc.Collections[collection.ID] != collection {
		t.Fatalf("expected collection with ID %q, got %+v", collection.ID, mc.Collections[collection.ID])
	}
	if mc.CollectionsMetadata[collection.ID] != collection.Metadata {
		t.Fatalf("expected collection metadata with ID %q, got %+v", collection.ID, mc.CollectionsMetadata[collection.ID])
	}

	// Test GetCollections
	collections, err := mc.GetCollections(ctx, collection.ID, &collection.Name, &collection.Topic)
	if err != nil {
		t.Fatalf("unexpected error getting collections: %v", err)
	}
	if len(collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(collections))
	}
	if collections[0] != collection {
		t.Fatalf("expected collection %+v, got %+v", collection, collections[0])
	}

	// Test DeleteCollection
	if err := mc.DeleteCollection(ctx, collection.ID); err != nil {
		t.Fatalf("unexpected error deleting collection: %v", err)
	}
}
