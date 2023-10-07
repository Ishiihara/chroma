package grpccoordinator

import (
	"context"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"pgregory.net/rapid"
)

func configDatabase() {
	dsn := "root:@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local&allowFallbackToPlaintext=true"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	dbcore.SetGlobalDB(db)
	db.Migrator().DropTable(&dbmodel.Collection{})
	db.Migrator().DropTable(&dbmodel.CollectionMetadata{})
	db.Migrator().CreateTable(&dbmodel.Collection{})
	db.Migrator().CreateTable(&dbmodel.CollectionMetadata{})
}

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
	// configDatabase()
	s, err := NewForTest(Config{})
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	var state []*coordinatorpb.Collection // model of the collection
	t.Repeat(map[string]func(*rapid.T){
		"create_collection": func(t *rapid.T) {
			collectionpb := rapid.Custom[*coordinatorpb.Collection](func(t *rapid.T) *coordinatorpb.Collection {
				return &coordinatorpb.Collection{
					Id:   rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id"),
					Name: rapid.String().Draw(t, "collection_name"),
				}
			}).Draw(t, "collectionpb")
			request := coordinatorpb.CreateCollectionRequest{
				Collection: collectionpb,
			}
			res, err := s.CreateCollection(context.Background(), &request)
			if err != nil {
				t.Fatalf("error creating collection: %v", err)
			}
			if res.Status.Code != 0 {
				t.Logf("error creating collection: %v", res.Status.Reason)
			}
			if res.Status.Reason != "" {
				t.Logf("error creating collection: %v", res.Status.Reason)
			}
			state = append(state, collectionpb)
		},
	})

}

func DrawMetadata(t *rapid.T) *rapid.Generator[model.MetadataValueType] {
	return rapid.OneOf[model.MetadataValueType](
		rapid.Custom[model.MetadataValueType](func(t *rapid.T) model.MetadataValueType {
			return &model.MetadataValueStringType{
				Value: rapid.String().Draw(t, "string_value"),
			}
		}),
		rapid.Custom[model.MetadataValueType](func(t *rapid.T) model.MetadataValueType {
			return &model.MetadataValueInt64Type{
				Value: rapid.Int64().Draw(t, "int_value"),
			}
		}),
		rapid.Custom[model.MetadataValueType](func(t *rapid.T) model.MetadataValueType {
			return &model.MetadataValueFloat64Type{
				Value: rapid.Float64().Draw(t, "float_value"),
			}
		}),
	)
}

func TestCollection(t *testing.T) {
	rapid.Check(t, testCollection)
}
