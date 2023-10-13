package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type SegmentMetadata struct {
	SegmentID  string          `gorm:"segment_id"`
	Key        string          `gorm:"key"`
	StrValue   string          `gorm:"str_value"`
	IntValue   *int64          `gorm:"int_value"`
	FloatValue *float64        `gorm:"float_value"`
	Ts         types.Timestamp `gorm:"ts"`
	CreatedAt  time.Time       `gorm:"created_at"`
	UpdatedAt  time.Time       `gorm:"updated_at"`
}

func (SegmentMetadata) TableName() string {
	return "segment_metadata"
}

//go:generate mockery --name=ISegmentMetadataDb
type ISegmentMetadataDb interface {
	Insert(in []*SegmentMetadata) error
}
