package dao

// import (
// 	"context"
// 	"database/sql"
// 	"os"
// 	"testing"

// 	"github.com/DATA-DOG/go-sqlmock"
// 	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
// 	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
// 	"github.com/milvus-io/milvus/pkg/util/typeutil"
// 	"github.com/zeebo/assert"
// 	"gorm.io/driver/mysql"
// 	"gorm.io/gorm"
// )

// const (
// 	tenantID   = "test_tenant"
// 	noTs       = typeutil.Timestamp(0)
// 	ts         = typeutil.Timestamp(10)
// 	collID1    = typeutil.UniqueID(101)
// 	segmentID1 = typeutil.UniqueID(2001)
// 	segmentID2 = typeutil.UniqueID(2002)
// 	NumRows    = 1025
// )

// var (
// 	mock       sqlmock.Sqlmock
// 	collTestDb dbmodel.ICollectionDb
// )

// func TestMain(m *testing.M) {
// 	var (
// 		db  *sql.DB
// 		err error
// 		ctx = context.TODO()
// 	)

// 	// setting sql MUST exact match
// 	db, mock, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
// 	if err != nil {
// 		panic(err)
// 	}

// 	DB, err := gorm.Open(mysql.New(mysql.Config{
// 		Conn:                      db,
// 		SkipInitializeWithVersion: true,
// 	}), &gorm.Config{})
// 	if err != nil {
// 		panic(err)
// 	}

// 	// set mocked database
// 	dbcore.SetGlobalDB(DB)

// 	collTestDb = NewMetaDomain().CollectionDb(ctx)

// 	// m.Run entry for executing tests
// 	os.Exit(m.Run())
// }

// func TestCollection_GetCidTs_Ts0(t *testing.T) {
// 	var collection = &dbmodel.Collection{
// 		CollectionID: collID1,
// 		Ts:           noTs,
// 	}

// 	// expectation
// 	mock.ExpectQuery("SELECT collection_id, ts FROM `collections` WHERE tenant_id = ? AND collection_id = ? AND ts <= ? ORDER BY ts desc LIMIT 1").
// 		WithArgs(tenantID, collID1, noTs).
// 		WillReturnRows(
// 			sqlmock.NewRows([]string{"collection_id", "ts"}).
// 				AddRow(collID1, noTs))

// 	// actual
// 	res, err := collTestDb.GetCollectionIDTs(tenantID, collID1, noTs)
// 	assert.NoError(t, err)
// 	assert.Equal(t, collection, res)
// }
