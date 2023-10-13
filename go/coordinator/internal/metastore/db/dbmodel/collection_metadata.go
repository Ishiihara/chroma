package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type CollectionMetadata struct {
	CollectionID string          `gorm:"collection_id"`
	Key          string          `gorm:"key"`
	StrValue     string          `gorm:"str_value"`
	IntValue     *int64          `gorm:"int_value"`
	FloatValue   *float64        `gorm:"float_value"`
	Ts           types.Timestamp `gorm:"ts"`
	CreatedAt    time.Time       `gorm:"created_at"`
	UpdatedAt    time.Time       `gorm:"updated_at"`
}

func (v CollectionMetadata) TableName() string {
	return "collection_metadata"
}

//go:generate mockery --name=ICollectionMetadataDb
type ICollectionMetadataDb interface {
	Insert(in []*CollectionMetadata) error
}
