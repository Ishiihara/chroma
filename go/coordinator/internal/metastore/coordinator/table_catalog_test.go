package coordinator

// import (
// 	"context"
// 	"os"
// 	"testing"

// 	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
// 	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel/mocks"
// 	"github.com/milvus-io/milvus/pkg/util/contextutil"
// 	"github.com/milvus-io/milvus/pkg/util/typeutil"
// 	"github.com/stretchr/testify/mock"
// 	"github.com/stretchr/testify/require"
// )

// const (
// 	tenantID      = "test_tenant"
// 	noTs          = typeutil.Timestamp(0)
// 	ts            = typeutil.Timestamp(10)
// 	collID1       = typeutil.UniqueID(101)
// 	partitionID1  = typeutil.UniqueID(500)
// 	fieldID1      = typeutil.UniqueID(1000)
// 	indexID1      = typeutil.UniqueID(1500)
// 	segmentID1    = typeutil.UniqueID(2000)
// 	indexBuildID1 = typeutil.UniqueID(3000)

// 	testDb = int64(1000)

// 	collName1  = "test_collection_name_1"
// 	collAlias1 = "test_collection_alias_1"
// 	collAlias2 = "test_collection_alias_2"

// 	username = "test_username_1"
// 	password = "test_xxx"
// )

// var (
// 	ctx            context.Context
// 	metaDomainMock *mocks.IMetaDomain
// 	collDbMock     *mocks.ICollectionDb
// 	mockCatalog    *Catalog
// )

// // TestMain is the first function executed in current package, we will do some initial here
// func TestMain(m *testing.M) {
// 	ctx = contextutil.WithTenantID(context.Background(), tenantID)

// 	collDbMock = &mocks.ICollectionDb{}

// 	metaDomainMock = &mocks.IMetaDomain{}
// 	metaDomainMock.On("CollectionDb", ctx).Return(collDbMock)

// 	mockCatalog = mockMetaCatalog(metaDomainMock)

// 	// m.Run entry for executing tests
// 	os.Exit(m.Run())
// }

// type NoopTransaction struct{}

// func (*NoopTransaction) Transaction(ctx context.Context, fn func(txctx context.Context) error) error {
// 	return fn(ctx)
// }

// func mockMetaCatalog(petDomain dbmodel.IMetaDomain) *Catalog {
// 	return NewTableCatalog(&NoopTransaction{}, petDomain)
// }

// func TestTableCatalog_CreateCollection(t *testing.T) {
// 	coll := &dbmodel.Collection{
// 		CollectionID:   collID1,
// 		CollectionName: collName1,
// 		Ts:             0,
// 	}

// 	// expectation
// 	collDbMock.On("Insert", mock.Anything).Return(nil).Once()

// 	// actual
// 	gotErr := mockCatalog.CreateCollection(ctx, coll, ts)
// 	require.Equal(t, nil, gotErr)
// }
