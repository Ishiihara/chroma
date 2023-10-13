package model

import "github.com/chroma/chroma-coordinator/internal/types"

type Collection struct {
	ID        types.UniqueID
	Name      string
	Topic     string
	Dimension int64
	Metadata  *CollectionMetadata[CollectionMetadataValueType]
	Ts        types.Timestamp
}

func FilterCollection(collection *Collection, collectionID types.UniqueID, collectionName *string, collectionTopic *string) bool {
	if collectionID != types.NilUniqueID() && collectionID != collection.ID {
		return false
	}
	if collectionName != nil && *collectionName != collection.Name {
		return false
	}
	if collectionTopic != nil && *collectionTopic != collection.Topic {
		return false
	}
	return true
}
