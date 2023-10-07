package dao

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
)

type metaDomain struct{}

func NewMetaDomain() *metaDomain {
	return &metaDomain{}
}

func (*metaDomain) CollectionDb(ctx context.Context) dbmodel.ICollectionDb {
	return &collectionDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) CollectionMetadataDb(ctx context.Context) dbmodel.ICollectionMetadataDb {
	return &collectionMetadataDb{dbcore.GetDB(ctx)}
}
