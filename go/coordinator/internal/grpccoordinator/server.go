package grpccoordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/coordinator"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator/grpcutils"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

type Config struct {
	// GRPC config
	BindAddress string

	// MetaTable config
	Username     string
	Password     string
	Address      string
	DBName       string
	MaxIdleConns int
	MaxOpenConns int
}

type Server struct {
	coordinatorpb.UnimplementedMetadataServiceServer
	coordinator  *coordinator.Coordinator
	grpcServer   grpcutils.GrpcServer
	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	return NewWithGrpcProvider(config, grpcutils.Default)
}

func NewForTest(config Config) (*Server, error) {
	ctx := context.Background()
	s := &Server{}
	coordinatorConfig := coordinator.CooordinatorConfig{
		BindAddress: config.BindAddress,
	}

	s.coordinator = coordinator.NewCoordinator(ctx, coordinatorConfig)
	err := s.coordinator.InitMetaTableForTest()
	if err != nil {
		return nil, err
	}
	s.coordinator.Start()
	return s, nil
}

func NewWithGrpcProvider(config Config, provider grpcutils.GrpcProvider) (*Server, error) {
	ctx := context.Background()

	s := &Server{
		healthServer: health.NewServer(),
	}

	coordinatorConfig := coordinator.CooordinatorConfig{
		BindAddress: config.BindAddress,
	}

	metaDBConfig := dbcore.MetaDBConfig{
		Username:     config.Username,
		Password:     config.Password,
		Address:      config.Address,
		DBName:       config.DBName,
		MaxIdleConns: config.MaxIdleConns,
		MaxOpenConns: config.MaxOpenConns,
	}

	s.coordinator = coordinator.NewCoordinator(ctx, coordinatorConfig)
	err := s.coordinator.InitMetaTable(metaDBConfig)
	if err != nil {
		return nil, err
	}
	s.coordinator.Start()

	// Start GRPC Server
	s.grpcServer, err = provider.StartGrpcServer("coordinator", config.BindAddress, func(registrar grpc.ServiceRegistrar) {
		coordinatorpb.RegisterMetadataServiceServer(registrar, s)
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	return nil
}
