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

func (s *Coordinator) CreateSegment(ctx context.Context, segment *model.Segment) error {
	if err := verifySegment(segment); err != nil {
		return err
	}
	err := s.meta.AddSegment(ctx, segment)
	if err != nil {
		return err
	}
	return nil
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

func verifyCollectionMetadata(metadata *model.CollectionMetadata[model.CollectionMetadataValueType]) error {
	for _, value := range metadata.Metadata {
		switch (value).(type) {
		case *model.CollectionMetadataValueStringType:
		case *model.CollectionMetadataValueInt64Type:
		case *model.CollectionMetadataValueFloat64Type:
		default:
			return ErrUnknownCollectionMetadataType
		}
	}
	return nil
}

func verifySegment(segment *model.Segment) error {
	if segment.ID.String() == "" {
		return errors.New("segment ID cannot be empty")
	}
	if segment.CollectionID.String() == "" {
		return errors.New("segment collection ID cannot be empty")
	}
	if err := verifySegmentMetadata(segment.Metadata); err != nil {
		return err
	}
	return nil
}

func verifySegmentMetadata(metadata *model.SegmentMetadata[model.SegmentMetadataValueType]) error {
	for _, value := range metadata.Metadata {
		switch (value).(type) {
		case *model.SegmentMetadataValueStringType:
		case *model.SegmentMetadataValueInt64Type:
		case *model.SegmentMetadataValueFloat64Type:
		default:
			return ErrUnknownSegmentMetadataType
		}
	}
	return nil
}
