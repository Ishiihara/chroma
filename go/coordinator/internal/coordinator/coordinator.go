package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/coordinator"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dao"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"gorm.io/gorm"
)

type Coordinator struct {
	ctx  context.Context
	meta *MetaTable
}

func NewCoordinator(ctx context.Context, db *gorm.DB) (*Coordinator, error) {
	s := &Coordinator{
		ctx: ctx,
	}

	catalog := coordinator.NewTableCatalog(dbcore.NewTxImpl(), dao.NewMetaDomain())
	meta, err := NewMetaTable(s.ctx, catalog)
	if err != nil {
		return nil, err
	}
	s.meta = meta

	return s, nil
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
