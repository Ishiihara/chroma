package coordinator

import (
	"context"
	"sync"

	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

type MetaTable struct {
	ddLock           sync.RWMutex
	ctx              context.Context
	catalog          metastore.Catalog
	collectionsCache map[types.UniqueID]*model.Collection
}

func NewMetaTable(ctx context.Context, catalog metastore.Catalog) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:              ctx,
		catalog:          catalog,
		collectionsCache: make(map[types.UniqueID]*model.Collection),
	}
	if err := mt.reload(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reload() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	oldCollections, err := mt.catalog.GetCollections(mt.ctx, types.NilUniqueID(), nil, nil)
	if err != nil {
		return err
	}
	// reload is idempotent
	mt.collectionsCache = make(map[types.UniqueID]*model.Collection)
	for _, collection := range oldCollections {
		mt.collectionsCache[types.UniqueID(collection.ID)] = collection
	}
	return nil
}

func (mt *MetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.catalog.CreateCollection(ctx, coll, coll.Ts); err != nil {
		return err
	}
	mt.collectionsCache[types.UniqueID(coll.ID)] = coll
	return nil
}

func (mt *MetaTable) GetCollections(ctx context.Context) ([]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	// Get the data from the cache
	collections := make([]*model.Collection, 0, len(mt.collectionsCache))
	for _, collection := range mt.collectionsCache {
		collections = append(collections, collection)
	}
	return collections, nil

}

func (mt *MetaTable) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.catalog.DeleteCollection(ctx, collectionID); err != nil {
		return err
	}
	delete(mt.collectionsCache, collectionID)
	return nil
}

func (mt *MetaTable) UpdateCollection(ctx context.Context, collection *model.Collection) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.catalog.UpdateCollection(ctx, collection, collection.Ts); err != nil {
		return err
	}
	mt.collectionsCache[types.UniqueID(collection.ID)] = collection
	return nil
}
