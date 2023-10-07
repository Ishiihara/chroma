package model

import "github.com/chroma/chroma-coordinator/internal/types"

type Collection struct {
	ID        types.UniqueID
	Name      string
	Topic     string
	Dimension int64
	Metadata  *CollectionMetadata[MetadataValueType]
	Ts        types.Timestamp
}
