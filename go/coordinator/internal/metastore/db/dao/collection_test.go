package dao

import (
	"testing"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCollectionDb_GetCollectionIDTs(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(&dbmodel.Collection{})
	assert.NoError(t, err)

	uniqueID := types.NewUniqueID()
	collection := &dbmodel.Collection{
		ID: uniqueID.String(),
	}
	err = db.Create(collection).Error
	assert.NoError(t, err)

	collectionDb := &collectionDb{
		db: db,
	}

	// Test when record is found
	col, err := collectionDb.GetCollectionIDTs(types.MustParse(collection.ID), types.MaxTimestamp)
	assert.NoError(t, err)
	assert.Equal(t, collection.ID, col.ID)

	// Test when record is not found
	col, err = collectionDb.GetCollectionIDTs(types.NewUniqueID(), types.MaxTimestamp)
	assert.Error(t, err)
	assert.Nil(t, col)
}

func TestCollectionDb_GetCollections(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(&dbmodel.Collection{}, &dbmodel.CollectionMetadata{})
	assert.NoError(t, err)

	collection := &dbmodel.Collection{
		ID:    types.NewUniqueID().String(),
		Name:  "test_name",
		Topic: "test_topic",
	}
	err = db.Create(collection).Error
	assert.NoError(t, err)

	metadata := &dbmodel.CollectionMetadata{
		CollectionID: collection.ID,
		Key:          "test",
		StrValue:     "test",
	}
	err = db.Create(metadata).Error
	assert.NoError(t, err)

	collectionDb := &collectionDb{
		db: db,
	}

	// Test when all parameters are nil
	collections, err := collectionDb.GetCollections(types.NilUniqueID(), nil, nil)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)
	assert.Equal(t, collection.Name, collections[0].Collection.Name)
	assert.Equal(t, collection.Topic, collections[0].Collection.Topic)
	assert.Len(t, collections[0].CollectionMetadata, 1)
	assert.Equal(t, metadata.Key, collections[0].CollectionMetadata[0].Key)
	assert.Equal(t, metadata.StrValue, collections[0].CollectionMetadata[0].StrValue)

	// Test when filtering by ID
	collections, err = collectionDb.GetCollections(types.MustParse(collection.ID), nil, nil)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)

	// Test when filtering by name
	collections, err = collectionDb.GetCollections(types.NilUniqueID(), &collection.Name, nil)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)

	// Test when filtering by topic
	collections, err = collectionDb.GetCollections(types.NilUniqueID(), nil, &collection.Topic)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)
}
