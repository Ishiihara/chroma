package grpccoordinator

import (
	"context"
	"errors"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/types"
)

func (s *Server) CreateCollection(ctx context.Context, req *coordinatorpb.CreateCollectionRequest) (*coordinatorpb.CreateCollectionResponse, error) {
	collectionpb := req.GetCollection()
	res := &coordinatorpb.CreateCollectionResponse{}
	res.Collection = collectionpb

	collection, err := convertToModel(collectionpb)
	if err != nil {
		res.Status = &coordinatorpb.Status{
			Reason: err.Error(),
			Code:   1,
		}
		return res, err
	}
	err = s.coordinator.CreateCollection(ctx, collection)

	if err != nil {
		res.Status = &coordinatorpb.Status{
			Reason: err.Error(),
			Code:   1,
		}

		return res, err
	}
	res.Status = &coordinatorpb.Status{
		Reason: "success",
		Code:   0,
	}
	return res, nil
}

func convertToModel(collectionpb *coordinatorpb.Collection) (*model.Collection, error) {
	collectionID, err := types.Parse(collectionpb.Id)
	if err != nil {
		return nil, err
	}

	metadatapb := collectionpb.Metadata

	metadata := model.NewCollectionMetadata[model.MetadataValueType]()
	if metadatapb != nil {
		for key, value := range metadatapb.Metadata {
			switch v := (value.Value).(type) {
			case *coordinatorpb.UpdateMetadataValue_StringValue:
				metadata.Add(key, &model.MetadataValueStringType{Value: v.StringValue})
			case *coordinatorpb.UpdateMetadataValue_IntValue:
				metadata.Add(key, &model.MetadataValueInt64Type{Value: v.IntValue})
			case *coordinatorpb.UpdateMetadataValue_FloatValue:
				metadata.Add(key, &model.MetadataValueFloat64Type{Value: v.FloatValue})
			default:
				return nil, errors.New("unknown metadata value type")
			}
		}
	}
	return &model.Collection{
		ID:       collectionID,
		Name:     collectionpb.Name,
		Metadata: metadata,
	}, nil
}
