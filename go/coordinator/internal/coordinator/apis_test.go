package coordinator

import (
	"context"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
	"pgregory.net/rapid"
)

func testCollection(t *rapid.T) {
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	c, err := NewCoordinator(ctx, db)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	t.Repeat(map[string]func(*rapid.T){
		"create_collection": func(t *rapid.T) {
			stringValue := generateStringMetadataValue(t)
			intValue := generateInt64MetadataValue(t)
			floatValue := generateFloat64MetadataValue(t)

			metadata := model.NewCollectionMetadata[model.MetadataValueType]()
			metadata.Add("string_value", stringValue)
			metadata.Add("int_value", intValue)
			metadata.Add("float_value", floatValue)

			collection := rapid.Custom[*model.Collection](func(t *rapid.T) *model.Collection {
				return &model.Collection{
					ID:       types.MustParse(rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id")),
					Name:     rapid.String().Draw(t, "collection_name"),
					Metadata: metadata,
				}
			}).Draw(t, "collection")

			err := c.CreateCollection(ctx, collection)
			if err != nil {
				if err == ErrCollectionNameEmpty && collection.Name == "" {
					t.Logf("expected error for empty collection name")
				} else if err == ErrCollectionTopicEmpty {
					t.Logf("expected error for empty collection topic")
					// TODO: check the topic name not empty
				} else {
					t.Fatalf("error creating collection: %v", err)
				}
			}
			if err == nil {
				// verify the correctness
				collectionList, err := c.GetCollections(ctx, collection.ID, nil, nil)
				if err != nil {
					t.Fatalf("error getting collections: %v", err)
				}
				if len(collectionList) != 1 {
					t.Fatalf("More than 1 collection with the same collection id")
				}
				for _, collectionReturned := range collectionList {
					if collection.ID != collectionReturned.ID {
						t.Fatalf("collection id is the right value")
					}
				}
				// state = append(state, collectionpb)
			}
		},
		"list_collections": func(t *rapid.T) {
		},
	})
}

func generateStringMetadataValue(t *rapid.T) model.MetadataValueType {
	return &model.MetadataValueStringType{
		Value: rapid.String().Draw(t, "string_value"),
	}
}

func generateInt64MetadataValue(t *rapid.T) model.MetadataValueType {
	return &model.MetadataValueInt64Type{
		Value: rapid.Int64().Draw(t, "int_value"),
	}
}

func generateFloat64MetadataValue(t *rapid.T) model.MetadataValueType {
	return &model.MetadataValueFloat64Type{
		Value: rapid.Float64().Draw(t, "float_value"),
	}
}

func TestAPIs(t *testing.T) {
	rapid.Check(t, testCollection)
}
