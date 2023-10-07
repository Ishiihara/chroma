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
				dbCollectionMetadata.IntValue = v.Value
			case *model.MetadataValueFloat64Type:
				dbCollectionMetadata.FloatValue = v.Value
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

func (tc *Catalog) ListCollections(ctx context.Context, ts types.Timestamp) ([]*model.Collection, error) {
	cidTsPairs, err := tc.metaDomain.CollectionDb(ctx).ListCollectionIDTs(ts)
	if err != nil {
		return nil, err
	}
	collections := make([]*model.Collection, 0, len(cidTsPairs))
	for _, cidTsPair := range cidTsPairs {
		collection := &model.Collection{
			ID:    types.MustParse(cidTsPair.ID), // Check if we should use it. It's a string in the db.
			Name:  cidTsPair.Name,
			Topic: cidTsPair.Topic,
			Ts:    cidTsPair.Ts,
		}
		collections = append(collections, collection)
	}

	return collections, nil
}

func (tc *Catalog) ListCollectionsAndMetadata(ctx context.Context, ts types.Timestamp) ([]*model.Collection, error) {
	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).ListCollectionAndMetadataDataTs(ts)
	if err != nil {
		return nil, err
	}
	collections := make([]*model.Collection, 0, len(collectionAndMetadataList))
	for _, collectionAndMetadata := range collectionAndMetadataList {
		collection := &model.Collection{
			ID:    types.MustParse(collectionAndMetadata.Collection.ID), // Check if we should use it. It's a string in the db.
			Name:  collectionAndMetadata.Collection.Name,
			Topic: collectionAndMetadata.Collection.Topic,
			Ts:    collectionAndMetadata.Collection.Ts,
		}
		// TODO: add metadata
		collections = append(collections, collection)
	}

	return collections, nil
}
