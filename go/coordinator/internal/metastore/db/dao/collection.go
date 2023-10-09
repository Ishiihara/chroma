package dao

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
)

type collectionDb struct {
	db *gorm.DB
}

func (s *collectionDb) GetCollectionIDTs(collectionID types.UniqueID, ts types.Timestamp) (*dbmodel.Collection, error) {
	var col dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("id, ts").Where("id = ? AND ts <= ?", collectionID.String(), ts).Order("ts desc").Take(&col).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		log.Warn("record not found", zap.String("collectionID", collectionID.String()), zap.Int64("ts", ts), zap.Error(err))
		return nil, fmt.Errorf("record not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection ts failed", zap.String("collectionID", collectionID.String()), zap.Int64("ts", ts), zap.Error(err))
		return nil, err
	}

	return &col, nil
}

func (s *collectionDb) GetCollections(id types.UniqueID, name *string, topic *string) ([]*dbmodel.CollectionAndMetadata, error) {
	var collections []*dbmodel.CollectionAndMetadata

	query := s.db.Table("collections").
		Select("collections.id, collections.name, collections.topic, collection_metadata.key, collection_metadata.str_value, collection_metadata.int_value, collection_metadata.float_value").
		Joins("LEFT JOIN collection_metadata ON collections.id = collection_metadata.collection_id").
		Order("collections.id")

	if id != types.NilUniqueID() {
		query = query.Where("collections.id = ?", id.String())
	}
	if topic != nil {
		query = query.Where("collections.topic = ?", *topic)
	}
	if name != nil {
		query = query.Where("collections.name = ?", *name)
	}

	rows, err := query.Rows()
	if err != nil {
		log.Error("get collections failed", zap.String("collectionID", id.String()), zap.String("collectionName", *name), zap.String("collectionTopic", *topic), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var currentCollectionID string = ""
	var metadata []*dbmodel.CollectionMetadata
	var currentCollection *dbmodel.CollectionAndMetadata

	for rows.Next() {
		var (
			collectionID    string
			collectionName  string
			collectionTopic string
			key             string
			strValue        string
			intValue        *int64
			floatValue      *float64
		)

		rows.Scan(&collectionID, &collectionName, &collectionTopic, &key, &strValue, &intValue, &floatValue)

		if collectionID != currentCollectionID {
			currentCollectionID = collectionID
			metadata = nil

			currentCollection = &dbmodel.CollectionAndMetadata{
				Collection: &dbmodel.Collection{
					ID:    collectionID,
					Name:  collectionName,
					Topic: collectionTopic,
				},
				CollectionMetadata: metadata,
			}

			if currentCollectionID != "" {
				collections = append(collections, currentCollection)
			}

		}
		collectionMetadata := &dbmodel.CollectionMetadata{
			Key:          key,
			CollectionID: collectionID,
		}
		if strValue != "" {
			collectionMetadata.StrValue = strValue
		}
		if intValue != nil {
			collectionMetadata.IntValue = intValue
		}
		if floatValue != nil {
			collectionMetadata.FloatValue = floatValue
		}
		metadata = append(metadata, collectionMetadata)
		currentCollection.CollectionMetadata = metadata

	}
	return collections, nil

}

func (s *collectionDb) Get(collectionID types.UniqueID, ts types.Timestamp) (*dbmodel.Collection, error) {
	var r dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Where("id = ? AND ts = ? AND is_deleted = false", collectionID, ts).Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("collection not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection by collection_id and ts failed", zap.String("collectionID", collectionID.String()), zap.Int64("ts", ts), zap.Error(err))
		return nil, err
	}

	return &r, nil
}

func (s *collectionDb) GetCollectionIDByName(collectionName string, ts types.Timestamp) (types.UniqueID, error) {
	var r dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("id").Where("name = ? AND ts <= ?", collectionName, ts).Order("ts desc").Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return types.NilUniqueID(), fmt.Errorf("get id by name not found, collName=%s, ts=%d", collectionName, ts)
	}
	if err != nil {
		log.Error("get id by name failed", zap.String("collName", collectionName), zap.Int64("ts", ts), zap.Error(err))
		return types.NilUniqueID(), err
	}

	return types.MustParse(r.ID), nil
}

// Insert used in create & drop collection, needs be an idempotent operation, so we use DoNothing strategy here so it will not throw exception for retry, equivalent to kv catalog
func (s *collectionDb) Insert(in *dbmodel.Collection) error {
	err := s.db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&in).Error

	if err != nil {
		log.Error("insert collection failed", zap.String("collectionID", in.ID), zap.Int64("ts", in.Ts), zap.Error(err))
		return err
	}

	return nil
}

func generateCollectionUpdatesWithoutID(in *dbmodel.Collection) map[string]interface{} {
	ret := map[string]interface{}{
		"id":         in.ID,
		"name":       in.Name,
		"topic":      in.Topic,
		"ts":         in.Ts,
		"is_deleted": in.IsDeleted,
		"created_at": in.CreatedAt,
		"updated_at": in.UpdatedAt,
	}
	return ret
}

func (s *collectionDb) Update(in *dbmodel.Collection) error {
	updates := generateCollectionUpdatesWithoutID(in)
	return s.db.Model(&dbmodel.Collection{}).Where("id = ?", in.ID).Updates(updates).Error
}
