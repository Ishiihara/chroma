package dao

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type segmentDb struct {
	db *gorm.DB
}

// Insert used in create & drop collection, needs be an idempotent operation, so we use DoNothing strategy here so it will not throw exception for retry, equivalent to kv catalog
func (s *segmentDb) Insert(in *dbmodel.Segment) error {
	err := s.db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&in).Error

	if err != nil {
		log.Error("insert segment failed", zap.String("segmentID", in.ID), zap.Int64("ts", in.Ts), zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentDb) GetSegments(id types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*dbmodel.SegmentAndMetadata, error) {
	var segments []*dbmodel.SegmentAndMetadata

	query := s.db.Table("segments").
		Select("segments.id, segments.collection_id, segments.type, segments.scope, segmenents.topic, segment_metadata.key, segment_metadata.str_value, segment_metadata.int_value, segment_metadata.float_value").
		Joins("LEFT JOIN segment_metadata ON segments.id = segment_metadata.segment_id").
		Order("segments.id")

	if id != types.NilUniqueID() {
		query = query.Where("id = ?", id)
	}
	if segmentType != nil {
		query = query.Where("type = ?", segmentType)
	}
	if scope != nil {
		query = query.Where("scope = ?", scope)
	}
	if topic != nil {
		query = query.Where("topic = ?", topic)
	}
	if collectionID != types.NilUniqueID() {
		query = query.Where("collection_id = ?", collectionID)
	}

	rows, err := query.Rows()
	if err != nil {
		log.Error("get segments failed", zap.String("segmentID", id.String()), zap.String("segmentType", *segmentType), zap.String("scope", *scope), zap.String("collectionTopic", *topic), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var currentSegmentID string = ""
	var metadata []*dbmodel.SegmentMetadata
	var currentSegment *dbmodel.SegmentAndMetadata

	for rows.Next() {
		var (
			segmentID    string
			collectionID string
			segmentType  string
			scope        string
			topic        string
			key          string
			strValue     string
			intValue     *int64
			floatValue   *float64
		)

		rows.Scan(&segmentID, &collectionID, &segmentType, &scope, &topic, &key, &strValue, &intValue, &floatValue)

		if segmentID != currentSegmentID {
			currentSegmentID = segmentID
			metadata = nil

			currentSegment = &dbmodel.SegmentAndMetadata{
				Segment: &dbmodel.Segment{
					ID:           segmentID,
					Type:         segmentType,
					Scope:        scope,
					Topic:        topic,
					CollectionID: collectionID,
				},
				SegmentMetadata: metadata,
			}

			if currentSegmentID != "" {
				segments = append(segments, currentSegment)
			}

		}
		segmentMetadata := &dbmodel.SegmentMetadata{
			Key:       key,
			SegmentID: segmentID,
		}
		if strValue != "" {
			segmentMetadata.StrValue = strValue
		}
		if intValue != nil {
			segmentMetadata.IntValue = intValue
		}
		if floatValue != nil {
			segmentMetadata.FloatValue = floatValue
		}
		metadata = append(metadata, segmentMetadata)
		currentSegment.SegmentMetadata = metadata

	}

	return segments, nil
}
