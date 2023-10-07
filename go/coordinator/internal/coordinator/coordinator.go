package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/coordinator"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dao"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type CooordinatorConfig struct {
	BindAddress string
}

type Coordinator struct {
	ctx  context.Context
	meta *MetaTable
}

func NewCoordinator(ctx context.Context, config CooordinatorConfig) *Coordinator {
	s := &Coordinator{
		ctx: ctx,
	}
	return s
}

func (s *Coordinator) InitMetaTable(dbConfig dbcore.MetaDBConfig) error {
	// connect to database

	err := dbcore.Connect(dbConfig)
	if err != nil {
		return err
	}

	catalog := coordinator.NewTableCatalog(dbcore.NewTxImpl(), dao.NewMetaDomain())

	if s.meta, err = NewMetaTable(s.ctx, catalog); err != nil {
		return err
	}

	return nil
}

func (s *Coordinator) InitMetaTableForTest() error {
	dsn := "root:@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local&allowFallbackToPlaintext=true"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	dbcore.SetGlobalDB(db)
	db.Migrator().DropTable(&dbmodel.Collection{})
	db.Migrator().DropTable(&dbmodel.CollectionMetadata{})
	db.Migrator().CreateTable(&dbmodel.Collection{})
	db.Migrator().CreateTable(&dbmodel.CollectionMetadata{})
	catalog := coordinator.NewTableCatalog(dbcore.NewTxImpl(), dao.NewMetaDomain())

	if s.meta, err = NewMetaTable(s.ctx, catalog); err != nil {
		return err
	}

	return nil
}

func (s *Coordinator) GetMetaTable() *MetaTable {
	return s.meta
}

func (s *Coordinator) Start() error {
	return nil
}

func (s *Coordinator) Close() error {
	return nil
}
