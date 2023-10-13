package grpccoordinator

import (
	"context"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/coordinator"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator/grpcutils"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"pgregory.net/rapid"
)

// Model: What are the system invariants?
// 1. Collection ID is unique
// 2. Collection ID is a UUID format
// 3. Collection name is unique
// 4. Collection name is a string
// 5. Collection metadata is a flat map and can be empty. It cannot be a nested structure.

// CreateCollection
// Collection created successfully are visible to ListCollections
// Collection created should have the right metadata, the metadata should be a flat map, with keys as strings and values as strings, ints, or floats
// Collection created should have the right name
// Collection created should have the right ID
// Collection created should have the right topic
// Collection created should have the right timestamp
func testCollection(t *rapid.T) {
	db := dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{Testing: true}, grpcutils.Default, db)
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	var state []*coordinatorpb.Collection
	var collectionsWithErrors []*coordinatorpb.Collection

	t.Repeat(map[string]func(*rapid.T){
		"create_collection": func(t *rapid.T) {
			stringValue := generateStringMetadataValue(t)
			intValue := generateInt64MetadataValue(t)
			floatValue := generateFloat64MetadataValue(t)

			collectionpb := rapid.Custom[*coordinatorpb.Collection](func(t *rapid.T) *coordinatorpb.Collection {
				return &coordinatorpb.Collection{
					Id:   rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id"),
					Name: rapid.String().Draw(t, "collection_name"),
					Metadata: &coordinatorpb.UpdateMetadata{
						Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
							"string_value": stringValue,
							"int_value":    intValue,
							"float_value":  floatValue,
						},
					},
				}
			}).Draw(t, "collection")

			createCollectionRequest := coordinatorpb.CreateCollectionRequest{
				Collection: collectionpb,
			}
			ctx := context.Background()
			_, err := s.CreateCollection(ctx, &createCollectionRequest)
			if err != nil {
				if err == coordinator.ErrCollectionNameEmpty && collectionpb.Name == "" {
					t.Logf("expected error for empty collection name")
					collectionsWithErrors = append(collectionsWithErrors, collectionpb)
				} else if err == coordinator.ErrCollectionTopicEmpty {
					t.Logf("expected error for empty collection topic")
					collectionsWithErrors = append(collectionsWithErrors, collectionpb)
					// TODO: check the topic name not empty
				} else {
					t.Fatalf("error creating collection: %v", err)
					collectionsWithErrors = append(collectionsWithErrors, collectionpb)
				}
			}

			getCollectionsRequest := coordinatorpb.GetCollectionsRequest{
				Id: &collectionpb.Id,
			}
			if err == nil {
				// verify the correctness
				GetCollectionsResponse, err := s.GetCollections(ctx, &getCollectionsRequest)
				if err != nil {
					t.Fatalf("error getting collections: %v", err)
				}
				collectionList := GetCollectionsResponse.GetCollections()
				if len(collectionList) != 1 {
					t.Fatalf("More than 1 collection with the same collection id")
				}
				for _, collection := range collectionList {
					if collection.Id != collectionpb.Id {
						t.Fatalf("collection id is the right value")
					}
				}
				state = append(state, collectionpb)
			}
		},
		"get_collections": func(t *rapid.T) {
		},
	})
}

func generateStringMetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_StringValue{
			StringValue: rapid.String().Draw(t, "string_value"),
		},
	}
}

func generateInt64MetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_IntValue{
			IntValue: rapid.Int64().Draw(t, "int_value"),
		},
	}
}

func generateFloat64MetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
			FloatValue: rapid.Float64().Draw(t, "float_value"),
		},
	}
}

func TestCollection(t *testing.T) {
	rapid.Check(t, testCollection)
}
