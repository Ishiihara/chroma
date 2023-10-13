package coordinator

import (
	"errors"
)

var (
	ErrCollectionIDFormat   = errors.New("collection id format error")
	ErrCollectionNameEmpty  = errors.New("collection name is empty")
	ErrCollectionTopicEmpty = errors.New("collection topic is empty")

	ErrUnknownCollectionMetadataType = errors.New("collection metadata value type not supported")
	ErrUnknownSegmentMetadataType    = errors.New("segment metadata value type not supported")
)
