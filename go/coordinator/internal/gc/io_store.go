package gc

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
)

type InputStore interface {
	Input(ctx context.Context) ([]CollectionState, error)
}

type OutputStore interface {
	Output(ctx context.Context, result TaskResult) error
}

type IOStore struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

var _ InputStore = (*IOStore)(nil)
var _ OutputStore = (*IOStore)(nil)

func NewIOStore(metaDomain dbmodel.IMetaDomain, txImpl dbmodel.ITransaction) *IOStore {
	return &IOStore{
		metaDomain: metaDomain,
		txImpl:     txImpl,
	}
}

func (d *IOStore) Input(ctx context.Context) ([]CollectionState, error) {
	// We need to change the metastore to support this, note that we only fetch deleted segments for now.
	status := dbmodel.SegmentStatusDropping
	segments, err := d.metaDomain.SegmentDb(ctx).GetSegments(types.NilUniqueID(), nil, nil, nil, types.NilUniqueID(), &status)
	if err != nil {
		return nil, err
	}

	collectionStateMap := make(map[string]CollectionState)
	for _, segment := range segments {
		if segment.Segment.CollectionID != nil {
			collectionId := *segment.Segment.CollectionID
			if _, ok := collectionStateMap[collectionId]; !ok {
				collectionStateMap[collectionId] = CollectionState{
					CollectionID: collectionId,
					Segments:     make(map[string]SegmentState),
				}
			}
			segmentStatus := SegmentStatus_Dropping
			collectionStateMap[collectionId].Segments[segment.Segment.ID] = SegmentState{
				SegmentID: segment.Segment.ID,
				Status:    &segmentStatus,
				//Path:      segment.Segment.Path,
				//Status:    &segment.Segment.Status,
			}
		}

	}
	collectionState := make([]CollectionState, 0)
	for _, state := range collectionStateMap {
		collectionState = append(collectionState, state)
	}
	return collectionState, nil
}

func (d *IOStore) Output(ctx context.Context, result TaskResult) error {
	return d.txImpl.Transaction(ctx, func(ctx context.Context) error {
		segmentIDs := make([]string, 0)
		for _, state := range result.States {
			for _, segmentState := range state.Segments {
				if *segmentState.Status == SegmentStatus_Dropped {
					segmentIDs = append(segmentIDs, segmentState.SegmentID)
				}
			}
		}
		if len(segmentIDs) > 0 {
			err := d.metaDomain.SegmentDb(ctx).UpdateSegmentStatus(segmentIDs, dbmodel.SegmentStatusDropped)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
