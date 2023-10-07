// Code generated by mockery v2.33.3. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	model "github.com/chroma/chroma-coordinator/internal/model"
)

// Catalog is an autogenerated mock type for the Catalog type
type Catalog struct {
	mock.Mock
}

// CreateCollection provides a mock function with given fields: ctx, collectionInfo, ts
func (_m *Catalog) CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts uint64) error {
	ret := _m.Called(ctx, collectionInfo, ts)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *model.Collection, uint64) error); ok {
		r0 = rf(ctx, collectionInfo, ts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ListCollections provides a mock function with given fields: ctx, ts
func (_m *Catalog) ListCollections(ctx context.Context, ts uint64) ([]*model.Collection, error) {
	ret := _m.Called(ctx, ts)

	var r0 []*model.Collection
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, uint64) ([]*model.Collection, error)); ok {
		return rf(ctx, ts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, uint64) []*model.Collection); ok {
		r0 = rf(ctx, ts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*model.Collection)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, uint64) error); ok {
		r1 = rf(ctx, ts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewCatalog creates a new instance of Catalog. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCatalog(t interface {
	mock.TestingT
	Cleanup(func())
}) *Catalog {
	mock := &Catalog{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
