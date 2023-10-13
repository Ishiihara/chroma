package coordinator

import (
	"context"
	"errors"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

func (s *Coordinator) CreateCollection(ctx context.Context, collection *model.Collection) error {
	if err := verifyCollection(collection); err != nil {
		return err
	}
	err := s.meta.AddCollection(ctx, collection)
	if err != nil {
		return err
	}
	return nil
}

func (s *Coordinator) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	return s.meta.GetCollections(ctx, collectionID, collectionName, collectionTopic)
}

func (s *Coordinator) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	return s.meta.DeleteCollection(ctx, collectionID)
}

func (s *Coordinator) UpdateCollection(ctx context.Context, collection *model.Collection) error {
	if err := verifyCollection(collection); err != nil {
		return err
	}
	return s.meta.UpdateCollection(ctx, collection)
}

func verifyCollection(collection *model.Collection) error {
	if collection.ID.String() == "" {
		return errors.New("collection ID cannot be empty")
	}
	if collection.Name == "" {
		return ErrCollectionNameEmpty
	}
	if collection.Topic == "" {
		return ErrCollectionTopicEmpty
	}
	if err := verifyCollectionMetadata(collection.Metadata); err != nil {
		return err
	}
	return nil
}

func verifyCollectionMetadata(metadata *model.CollectionMetadata[model.MetadataValueType]) error {
	for _, value := range metadata.Metadata {
		switch (value).(type) {
		case *model.MetadataValueStringType:
		case *model.MetadataValueInt64Type:
		case *model.MetadataValueFloat64Type:
		default:
			return ErrUnknownMetadataType
		}
	}
	return nil
}
