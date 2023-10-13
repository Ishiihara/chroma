package grpccoordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/coordinator"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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

func (s *Server) GetCollections(ctx context.Context, req *coordinatorpb.GetCollectionsRequest) (*coordinatorpb.GetCollectionsResponse, error) {
	collectionID := req.GetId()
	collectionName := req.Name
	collectionTopic := req.Topic

	res := &coordinatorpb.GetCollectionsResponse{}
	parsedCollectionID, err := types.Parse(collectionID)
	if err != nil {
		res.Status = &coordinatorpb.Status{
			Reason: coordinator.ErrCollectionIDFormat.Error(),
			Code:   1,
		}
		return res, coordinator.ErrCollectionIDFormat
	}
	collections, err := s.coordinator.GetCollections(ctx, parsedCollectionID, collectionName, collectionTopic)
	if err != nil {
		res.Status = &coordinatorpb.Status{
			Reason: err.Error(),
			Code:   1,
		}
		return res, err
	}
	res.Collections = make([]*coordinatorpb.Collection, 0, len(collections))
	for _, collection := range collections {
		collectionpb := convertToProto(collection)
		res.Collections = append(res.Collections, collectionpb)
	}
	return res, nil
}

func convertToModel(collectionpb *coordinatorpb.Collection) (*model.Collection, error) {
	collectionID, err := types.Parse(collectionpb.Id)
	if err != nil {
		log.Error("collection id format error", zap.String("collectionpd.id", collectionpb.Id))
		return nil, coordinator.ErrCollectionIDFormat
	}

	metadatapb := collectionpb.Metadata
	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	if metadatapb != nil {
		for key, value := range metadatapb.Metadata {
			switch v := (value.Value).(type) {
			case *coordinatorpb.UpdateMetadataValue_StringValue:
				metadata.Add(key, &model.CollectionMetadataValueStringType{Value: v.StringValue})
			case *coordinatorpb.UpdateMetadataValue_IntValue:
				metadata.Add(key, &model.CollectionMetadataValueInt64Type{Value: v.IntValue})
			case *coordinatorpb.UpdateMetadataValue_FloatValue:
				metadata.Add(key, &model.CollectionMetadataValueFloat64Type{Value: v.FloatValue})
			default:
				log.Error("collection metadata value type not supported", zap.Any("metadata value", value))
				return nil, coordinator.ErrUnknownCollectionMetadataType
			}
		}
	}
	return &model.Collection{
		ID:       collectionID,
		Name:     collectionpb.Name,
		Metadata: metadata,
	}, nil
}

func convertToProto(collection *model.Collection) *coordinatorpb.Collection {
	metadatapb := &coordinatorpb.UpdateMetadata{}
	for key, value := range collection.Metadata.Metadata {
		switch v := (value).(type) {
		case *model.CollectionMetadataValueStringType:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_StringValue{
					StringValue: v.Value,
				},
			}
		case *model.CollectionMetadataValueInt64Type:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_IntValue{
					IntValue: v.Value,
				},
			}
		case *model.CollectionMetadataValueFloat64Type:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
					FloatValue: v.Value,
				},
			}
		}
	}
	return &coordinatorpb.Collection{
		Id:       collection.ID.String(),
		Name:     collection.Name,
		Metadata: metadatapb,
	}
}
