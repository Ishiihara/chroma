package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type Segment struct {
	ID           string    `gorm:"type:varchar(36);primaryKey"`
	Type         string    `gorm:"type:text;not null"`
	Scope        string    `gorm:"type:text;not null"`
	Topic        string    `gorm:"type:text"`
	Ts           int64     `gorm:"type:bigint unsigned;default:0"`
	IsDeleted    bool      `gorm:"default:false"`
	CreatedAt    time.Time `gorm:"not null"`
	UpdatedAt    time.Time `gorm:"not null"`
	CollectionID string    `gorm:"type:varchar(36);not null"`
}

func (s Segment) TableName() string {
	return "segments"
}

type SegmentAndMetadata struct {
	Segment         *Segment
	SegmentMetadata []*SegmentMetadata
}

//go:generate mockery --name=ISegmentDb
type ISegmentDb interface {
	GetSegments(id types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*SegmentAndMetadata, error)
	Insert(*Segment) error
}
