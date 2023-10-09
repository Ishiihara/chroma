package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

type Catalog struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

func NewTableCatalog(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *Catalog {
	return &Catalog{
		txImpl:     txImpl,
		metaDomain: metaDomain,
	}
}

func (tc *Catalog) CreateCollection(ctx context.Context, collection *model.Collection, ts types.Timestamp) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert collection
		dbCollection := &dbmodel.Collection{
			ID:    collection.ID.String(),
			Name:  collection.Name,
			Topic: collection.Topic,
			Ts:    ts,
		}
		err := tc.metaDomain.CollectionDb(txCtx).Insert(dbCollection)
		if err != nil {
			return err
		}

		// insert collection metadata
		metadata := collection.Metadata
		dbCollectionMetadataList := make([]*dbmodel.CollectionMetadata, 0, len(metadata.Metadata))
		for key, value := range metadata.Metadata {
			dbCollectionMetadata := &dbmodel.CollectionMetadata{
				CollectionID: dbCollection.ID,
				Key:          key,
				Ts:           ts,
			}
			switch v := (value).(type) {
			case *model.MetadataValueStringType:
				dbCollectionMetadata.StrValue = v.Value
			case *model.MetadataValueInt64Type:
				dbCollectionMetadata.IntValue = &v.Value
			case *model.MetadataValueFloat64Type:
				dbCollectionMetadata.FloatValue = &v.Value
			default:
				// TODO: should we throw an error here?
				continue
			}
			dbCollectionMetadataList = append(dbCollectionMetadataList, dbCollectionMetadata)
		}
		if len(dbCollectionMetadataList) != 0 {
			err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (tc *Catalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	collectionAndMetadatList, err := tc.metaDomain.CollectionDb(ctx).GetCollections(collectionID, collectionName, collectionTopic)
	if err != nil {
		return nil, err
	}
	collections := make([]*model.Collection, 0, len(collectionAndMetadatList))
	for _, collectionAndMetadata := range collectionAndMetadatList {
		collection := &model.Collection{
			ID:    types.MustParse(collectionAndMetadata.Collection.ID),
			Name:  collectionAndMetadata.Collection.Name,
			Topic: collectionAndMetadata.Collection.Topic,
			Ts:    collectionAndMetadata.Collection.Ts,
		}
		metadata := model.NewCollectionMetadata[model.MetadataValueType]()
		for _, collectionMetadata := range collectionAndMetadata.CollectionMetadata {
			switch {
			case collectionMetadata.StrValue != "":
				metadata.Add(collectionMetadata.Key, &model.MetadataValueStringType{Value: collectionMetadata.StrValue})
			case collectionMetadata.IntValue != nil:
				metadata.Add(collectionMetadata.Key, &model.MetadataValueInt64Type{Value: *collectionMetadata.IntValue})
			case collectionMetadata.FloatValue != nil:
				metadata.Add(collectionMetadata.Key, &model.MetadataValueFloat64Type{Value: *collectionMetadata.FloatValue})
			}
		}
		collection.Metadata = metadata
		collections = append(collections, collection)
	}
	return collections, nil
}

func (tc *Catalog) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Get the collection
		// TODO: introduce a new method to get a collection by ID without metadata
		collections, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(collectionID, nil, nil)
		if err != nil {
			return err
		}

		// Soft Delete
		for _, collection := range collections {
			collection.Collection.IsDeleted = true
			err := tc.metaDomain.CollectionDb(txCtx).Update(collection.Collection)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (tc *Catalog) UpdateCollection(ctx context.Context, collection *model.Collection, ts types.Timestamp) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Get the collection
		// insert collection
		dbCollection := &dbmodel.Collection{
			ID:    collection.ID.String(),
			Name:  collection.Name,
			Topic: collection.Topic,
			Ts:    ts,
		}
		err := tc.metaDomain.CollectionDb(txCtx).Insert(dbCollection)
		if err != nil {
			return err
		}

		// insert collection metadata
		metadata := collection.Metadata
		dbCollectionMetadataList := make([]*dbmodel.CollectionMetadata, 0, len(metadata.Metadata))
		for key, value := range metadata.Metadata {
			dbCollectionMetadata := &dbmodel.CollectionMetadata{
				CollectionID: dbCollection.ID,
				Key:          key,
				Ts:           ts,
			}
			switch v := (value).(type) {
			case *model.MetadataValueStringType:
				dbCollectionMetadata.StrValue = v.Value
			case *model.MetadataValueInt64Type:
				dbCollectionMetadata.IntValue = &v.Value
			case *model.MetadataValueFloat64Type:
				dbCollectionMetadata.FloatValue = &v.Value
			default:
				// TODO: should we throw an error here?
				continue
			}
			dbCollectionMetadataList = append(dbCollectionMetadataList, dbCollectionMetadata)
		}
		if len(dbCollectionMetadataList) != 0 {
			err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
