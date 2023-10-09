package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type Collection struct {
	ID        string          `gorm:"id"`
	Name      string          `gorm:"name"`
	Topic     string          `gorm:"topic"`
	Ts        types.Timestamp `gorm:"ts"`
	IsDeleted bool            `gorm:"is_deleted"`
	CreatedAt time.Time       `gorm:"created_at"`
	UpdatedAt time.Time       `gorm:"updated_at"`
}

func (v Collection) TableName() string {
	return "collections"
}

//go:generate mockery --name=ICollectionDb
type ICollectionDb interface {
	// GetCollectionIdTs get the largest timestamp that less than or equal to param ts, no matter is_deleted is true or false.
	GetCollectionIDTs(collectionID types.UniqueID, ts types.Timestamp) (*Collection, error)
	GetCollections(collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*CollectionAndMetadata, error)
	Get(collectionID types.UniqueID, ts types.Timestamp) (*Collection, error)
	GetCollectionIDByName(collectionName string, ts types.Timestamp) (types.UniqueID, error)
	Insert(in *Collection) error
	Update(in *Collection) error
}

type CollectionAndMetadata struct {
	Collection         *Collection
	CollectionMetadata []*CollectionMetadata
}
