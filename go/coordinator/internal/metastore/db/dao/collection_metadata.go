package dao

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type collectionMetadataDb struct {
	db *gorm.DB
}

func (s *collectionMetadataDb) Insert(in []*dbmodel.CollectionMetadata) error {
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "collection_id"}, {Name: "key"}, {Name: "ts"}},
		DoUpdates: clause.AssignmentColumns([]string{"str_value", "int_value", "float_value"}),
	}).Create(in).Error
}
