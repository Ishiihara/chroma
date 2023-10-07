package dbmodel

import "context"

//go:generate mockery --name=IMetaDomain
type IMetaDomain interface {
	CollectionDb(ctx context.Context) ICollectionDb
	CollectionMetadataDb(ctx context.Context) ICollectionMetadataDb
}

type ITransaction interface {
	Transaction(ctx context.Context, fn func(txCtx context.Context) error) error
}
