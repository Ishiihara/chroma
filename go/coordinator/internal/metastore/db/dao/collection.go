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

	err := s.db.Model(&dbmodel.Collection{}).Select("id, ts").Where("id = ? AND ts <= ?", collectionID, ts).Order("ts desc").Take(&col).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		log.Warn("record not found", zap.String("collectionID", collectionID.String()), zap.Uint64("ts", ts), zap.Error(err))
		return nil, fmt.Errorf("record not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection ts failed", zap.String("collectionID", collectionID.String()), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return &col, nil
}

func (s *collectionDb) ListCollectionIDTs(ts types.Timestamp) ([]*dbmodel.Collection, error) {
	var r []*dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("id, MAX(ts) ts").Where("ts <= ?", ts).Group("id").Find(&r).Error
	if err != nil {
		log.Error("list id & latest ts pairs in collections failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collectionDb) ListCollectionAndMetadataDataTs(ts types.Timestamp) ([]*dbmodel.CollectionAndMetadata, error) {
	var r []*dbmodel.CollectionAndMetadata

	err := s.db.Model(&dbmodel.Collection{}).
		Joins("LEFT JOIN collection_metadata on collections.id = collection_metadata.collection_id").
		Where("ts <= ?", ts).
		Order("collections.id").
		Find(&r).Error

	if err != nil {
		log.Error("list collections and metadata failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collectionDb) Get(collectionID types.UniqueID, ts types.Timestamp) (*dbmodel.Collection, error) {
	var r dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Where("id = ? AND ts = ? AND is_deleted = false", collectionID, ts).Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("collection not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection by collection_id and ts failed", zap.String("collectionID", collectionID.String()), zap.Uint64("ts", ts), zap.Error(err))
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
		log.Error("get id by name failed", zap.String("collName", collectionName), zap.Uint64("ts", ts), zap.Error(err))
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
		log.Error("insert collection failed", zap.String("collectionID", in.ID), zap.Uint64("ts", in.Ts), zap.Error(err))
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
