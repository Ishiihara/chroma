package coordinator

import (
	"context"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel/mocks"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCatalog_CreateCollection(t *testing.T) {
	// create a mock transaction implementation
	mockTxImpl := &mocks.ITransaction{}

	// create a mock meta domain implementation
	mockMetaDomain := &mocks.IMetaDomain{}

	// create a new catalog instance
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain)

	// create a mock collection
	metadata := model.NewCollectionMetadata[model.MetadataValueType]()
	metadata.Add("test_key", &model.MetadataValueStringType{Value: "test_value"})
	collection := &model.Collection{
		ID:       types.MustParse("00000000-0000-0000-0000-000000000001"),
		Name:     "test_collection",
		Topic:    "test_topic",
		Metadata: metadata,
	}

	// create a mock timestamp
	ts := types.Timestamp(1234567890)

	// mock the insert collection method
	mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("Insert", &dbmodel.Collection{
		ID:    "00000000-0000-0000-0000-000000000001",
		Name:  "test_collection",
		Topic: "test_topic",
		Ts:    ts,
	}).Return(nil)

	// mock the insert collection metadata method
	mockMetaDomain.On("CollectionMetadataDb", context.Background()).Return(&mocks.ICollectionMetadataDb{})
	mockMetaDomain.CollectionMetadataDb(context.Background()).(*mocks.ICollectionMetadataDb).On("Insert", []*dbmodel.CollectionMetadata{
		{
			CollectionID: "00000000-0000-0000-0000-000000000001",
			Key:          "test_key",
			StrValue:     "test_value",
			Ts:           ts,
		},
	}).Return(nil)

	// call the CreateCollection method
	err := catalog.CreateCollection(context.Background(), collection, ts)

	// assert that the method returned no error
	assert.NoError(t, err)

	// assert that the mock methods were called as expected
	mockMetaDomain.AssertExpectations(t)
}

func TestCatalog_GetCollections(t *testing.T) {
	// create a mock meta domain implementation
	mockMetaDomain := &mocks.IMetaDomain{}

	// create a new catalog instance
	catalog := NewTableCatalog(nil, mockMetaDomain)

	// create a mock collection ID
	collectionID := types.MustParse("00000000-0000-0000-0000-000000000001")

	// create a mock collection name
	collectionName := "test_collection"

	// create a mock collection topic
	collectionTopic := "test_topic"

	// create a mock collection and metadata list
	collectionAndMetadataList := []*dbmodel.CollectionAndMetadata{
		{
			Collection: &dbmodel.Collection{
				ID:    "00000000-0000-0000-0000-000000000001",
				Name:  "test_collection",
				Topic: "test_topic",
				Ts:    types.Timestamp(1234567890),
			},
			CollectionMetadata: []*dbmodel.CollectionMetadata{
				{
					CollectionID: "00000000-0000-0000-0000-000000000001",
					Key:          "test_key",
					StrValue:     "test_value",
					Ts:           types.Timestamp(1234567890),
				},
			},
		},
	}

	// mock the get collections method
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("GetCollections", collectionID, &collectionName, &collectionTopic).Return(collectionAndMetadataList, nil)

	// call the GetCollections method
	collections, err := catalog.GetCollections(context.Background(), collectionID, &collectionName, &collectionTopic)

	// assert that the method returned no error
	assert.NoError(t, err)

	// assert that the collections were returned as expected
	metadata := model.NewCollectionMetadata[model.MetadataValueType]()
	metadata.Add("test_key", &model.MetadataValueStringType{Value: "test_value"})
	assert.Equal(t, []*model.Collection{
		{
			ID:       types.MustParse("00000000-0000-0000-0000-000000000001"),
			Name:     "test_collection",
			Topic:    "test_topic",
			Ts:       types.Timestamp(1234567890),
			Metadata: metadata,
		},
	}, collections)

	// assert that the mock methods were called as expected
	mockMetaDomain.AssertExpectations(t)
}

// func TestCatalog_DeleteCollection(t *testing.T) {
// 	// create a mock transaction implementation
// 	mockTxImpl := &mocks.ITransaction{}

// 	// create a mock meta domain implementation
// 	mockMetaDomain := &mocks.IMetaDomain{}

// 	// create a new catalog instance
// 	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain)

// 	// create a mock collection ID
// 	collectionID := types.MustParse("00000000-0000-0000-0000-000000000001")

// 	// mock the delete collection method
// 	mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
// 	mockTxImpl.Transaction(context.Background(), mock.Anything).Run(func(args mock.Arguments) {
// 		fn := args.Get(0).(func(context.Context) error)
// 		err := fn(context.Background())
// 		assert.NoError(t, err)
// 	}).Once()

// 	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
// 	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("Delete", "00000000-0000-0000-0000-000000000001").Return(nil)

// 	// call the DeleteCollection method
// 	err := catalog.DeleteCollection(context.Background(), collectionID)

// 	// assert that the method returned no error
// 	assert.NoError(t, err)

// 	// assert that the mock methods were called as expected
// 	mockTxImpl.AssertExpectations(t)
// 	mockMetaDomain.AssertExpectations(t)
// }
