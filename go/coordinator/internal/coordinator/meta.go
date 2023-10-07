package coordinator

import (
	"context"
	"sync"

	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type MetaTable struct {
	ctx         context.Context
	catalog     metastore.Catalog
	collID2Meta map[types.UniqueID]*model.Collection // collection id -> collection meta
	ddLock      sync.RWMutex
}

func NewMetaTable(ctx context.Context, catalog metastore.Catalog) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:         ctx,
		catalog:     catalog,
		collID2Meta: make(map[types.UniqueID]*model.Collection),
	}
	if err := mt.reload(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reload() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.reloadWithNonDatabase(); err != nil {
		return err
	}
	return nil
}

// insert into default database if the collections doesn't inside some database
func (mt *MetaTable) reloadWithNonDatabase() error {
	collectionNum := int64(0)
	oldCollections, err := mt.catalog.ListCollections(mt.ctx, types.MaxTimestamp)
	if err != nil {
		return err
	}

	for _, collection := range oldCollections {
		mt.collID2Meta[types.UniqueID(collection.ID)] = collection
	}

	if collectionNum > 0 {
		log.Info("recover collections without db", zap.Int64("collection_num", collectionNum))
	}
	return nil
}

func (mt *MetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.catalog.CreateCollection(ctx, coll, coll.Ts); err != nil {
		return err
	}

	// mt.collID2Meta[coll.CollectionID] = coll.Clone()
	mt.collID2Meta[types.UniqueID(coll.ID)] = coll
	return nil
}
