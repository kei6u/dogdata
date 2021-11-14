package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	dogdatapb "github.com/kei6u/dogdata/proto/v1/dogdata"
	healthcheckpb "github.com/kei6u/dogdata/proto/v1/healthcheck"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var _ dogdatapb.DogDataServiceServer = (*server)(nil)
var _ healthcheckpb.HealthCheckServiceServer = (*server)(nil)

type server struct {
	// gRPCAddr is the address where gRPC server listens to.
	gRPCAddr string
	// gRPCGWAddr is the address where gRPC-Gateway server listens to.
	gRPCGWAddr string
	logger     *zap.Logger
	db         *sql.DB
}

func newServer(gRPCAddr, gRPCGWAddr string, logger *zap.Logger, db *sql.DB) *server {
	if !strings.HasPrefix(gRPCAddr, ":") {
		gRPCAddr = fmt.Sprintf(":%s", gRPCAddr)
	}
	if !strings.HasPrefix(gRPCGWAddr, ":") {
		gRPCGWAddr = fmt.Sprintf(":%s", gRPCGWAddr)
	}
	return &server{
		gRPCAddr:   gRPCAddr,
		gRPCGWAddr: gRPCGWAddr,
		logger:     logger,
		db:         db,
	}
}

func (s *server) Start(ctx context.Context) {
	grpcMetrics := grpc_prometheus.NewServerMetrics()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := prometheus.NewRegistry()
		if err := r.Register(grpcMetrics); err != nil {
			s.logger.Warn("failed to register gRPC metrics to prometheus", zap.Error(err))
			return
		}
		if err := (&http.Server{
			Handler: promhttp.HandlerFor(r, promhttp.HandlerOpts{}),
			Addr:    ":9092",
		}).ListenAndServe(); err != nil {
			s.logger.Warn("prometheus server fails to start", zap.Error(err))
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lis, err := net.Listen("tcp", s.gRPCAddr)
		if err != nil {
			s.logger.Warn("faild to listen to gRPC Address", zap.Error(err), zap.String("address", s.gRPCAddr))
			return
		}
		defer lis.Close()

		grpcsvc := grpc.NewServer(
			grpc_middleware.WithUnaryServerChain(
				grpc_recovery.UnaryServerInterceptor(),
				grpcMetrics.UnaryServerInterceptor(),
				grpc_opentracing.UnaryServerInterceptor(
					grpc_opentracing.WithTracer(opentracer.New(tracer.WithAnalytics(true))),
					grpc_opentracing.WithFilterFunc(func(_ context.Context, fullMethodName string) bool {
						return !strings.Contains(fullMethodName, "healthcheck")
					}),
				),
				grpc_zap.UnaryServerInterceptor(
					s.logger,
					grpc_zap.WithDecider(func(fullMethodName string, _ error) bool {
						return !strings.Contains(fullMethodName, "healthcheck")
					}),
					grpc_zap.WithMessageProducer(func(ctx context.Context, msg string, level zapcore.Level, code codes.Code, err error, duration zapcore.Field) {
						// inject trace ID into logs
						if dds, ok := tracer.SpanFromContext(ctx); ok {
							grpc_zap.AddFields(
								ctx,
								zap.Uint64("dd.trace_id", dds.Context().TraceID()),
								zap.Uint64("dd.span_id", dds.Context().SpanID()),
							)
						}
						grpc_zap.DefaultMessageProducer(ctx, msg, level, code, err, duration)
					}),
				),
			),
		)
		dogdatapb.RegisterDogDataServiceServer(grpcsvc, s)
		healthcheckpb.RegisterHealthCheckServiceServer(grpcsvc, s)
		grpcMetrics.InitializeMetrics(grpcsvc)

		s.logger.Info("gRPC server starts", zap.String("address", s.gRPCAddr))
		if err := grpcsvc.Serve(lis); err != nil {
			s.logger.Warn("gRPC server fails to start", zap.Error(err), zap.String("address", s.gRPCAddr))
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := grpc.DialContext(
			ctx,
			s.gRPCAddr,
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithDisableHealthCheck(),
		)
		if err != nil {
			s.logger.Warn("failed to dial gRPC server", zap.Error(err))
			return
		}
		defer conn.Close()

		gwmux := runtime.NewServeMux()
		if err := dogdatapb.RegisterDogDataServiceHandler(ctx, gwmux, conn); err != nil {
			s.logger.Warn("failed to regiser handler", zap.Error(err))
			return
		}
		if err := healthcheckpb.RegisterHealthCheckServiceHandler(ctx, gwmux, conn); err != nil {
			s.logger.Warn("failed to regiser handler", zap.Error(err))
			return
		}

		s.logger.Info("gRPC-Gateway server starts", zap.String("address", s.gRPCGWAddr))
		if err := (&http.Server{
			Addr:    s.gRPCGWAddr,
			Handler: gwmux,
		}).ListenAndServe(); err != nil {
			s.logger.Warn("gRPC-Gateway fails to start", zap.Error(err))
			return
		}
	}()

	wg.Wait()
}

func (s *server) Create(ctx context.Context, req *dogdatapb.CreateRequest) (*dogdatapb.CreateResponse, error) {
	// Validating duplicated data.
	row := s.db.QueryRowContext(
		ctx,
		"SELECT name FROM dogdata WHERE name=$1 AND breed=$2",
		req.GetName(), req.GetBreed(),
	)
	if row == nil {
		return nil, status.Error(codes.Internal, "failed to validate the existence of requested data")
	}
	var name string
	err := row.Scan(&name)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "failed to validate the existence of requested data: %s", err)
	}
	if name != "" {
		return nil, status.Errorf(codes.AlreadyExists, "%s is already existed", name)
	}

	// Creating new data.
	var id int
	if err := s.db.QueryRowContext(
		ctx,
		"INSERT INTO dogdata (name, breed) VALUES ($1, $2) RETURNING id",
		req.GetName(), req.GetBreed(),
	).Scan(&id); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create new data: %s", err)
	}
	return &dogdatapb.CreateResponse{
		Id: int32(id),
	}, nil
}

func (s *server) Get(ctx context.Context, req *dogdatapb.GetRequest) (*dogdatapb.GetResponse, error) {
	var id int
	var name string
	var breed string
	err := s.db.QueryRowContext(
		ctx,
		"SELECT id, name, breed FROM dogdata WHERE id=$1",
		req.GetId(),
	).Scan(&id, &name, &breed)
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "data associated requested id is not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch data associated requested id: %s", err)
	}
	return &dogdatapb.GetResponse{
		Dogdata: &dogdatapb.DogData{
			Id:    int32(id),
			Name:  name,
			Breed: breed,
		},
	}, nil
}

func (s *server) List(ctx context.Context, req *dogdatapb.ListRequest) (*dogdatapb.ListResponse, error) {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT * FROM dogdata",
	)
	defer rows.Close()
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "not data exists")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list data: %s", err)
	}
	var dds []*dogdatapb.DogData
	for rows.Next() {
		var dd dogdatapb.DogData
		if err := rows.Scan(&dd.Id, &dd.Name, &dd.Breed); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list data: %s", err)
		}
		dds = append(dds, &dd)
	}
	if err = rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list data: %s", err)
	}
	return &dogdatapb.ListResponse{Dogdata: dds}, nil
}

func (s *server) Update(ctx context.Context, req *dogdatapb.UpdateRequest) (*dogdatapb.UpdateResponse, error) {
	existed, err := s.Get(ctx, &dogdatapb.GetRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	existed.Dogdata.Name = req.GetName()

	// Validating duplicated data.
	var name string
	err = s.db.QueryRowContext(
		ctx,
		"SELECT name FROM dogdata WHERE name=$1 AND breed =$2",
		existed.GetDogdata().GetName(), existed.GetDogdata().GetBreed(),
	).Scan(&name)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "failed to validate the existence of requested data: %s", err)
	}
	if name != "" {
		return nil, status.Errorf(codes.AlreadyExists, "%s is already existed", name)
	}

	// Updating existed data.
	res, err := s.db.ExecContext(
		ctx,
		"UPDATE dogdata SET name = $1 WHERE id = $2",
		existed.GetDogdata().GetName(), existed.GetDogdata().GetId(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update data: %s", err)
	}
	if _, err := res.RowsAffected(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update data: %s", err)
	}
	return &dogdatapb.UpdateResponse{Dogdata: existed.GetDogdata()}, nil
}

func (s *server) Delete(ctx context.Context, req *dogdatapb.DeleteRequest) (*dogdatapb.DeleteResponse, error) {
	_, err := s.db.ExecContext(
		ctx,
		"DELETE FROM dogdata WHERE id = $1",
		req.GetId(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete data: %s", err)
	}
	return &dogdatapb.DeleteResponse{}, nil
}

func (s *server) LivenessProbe(context.Context, *healthcheckpb.LivenessProbeRequest) (*healthcheckpb.LivenessProbeResponse, error) {
	return &healthcheckpb.LivenessProbeResponse{}, nil
}

func (s *server) ReadinessProbe(context.Context, *healthcheckpb.ReadinessProbeRequest) (*healthcheckpb.ReadinessProbeResponse, error) {
	if err := s.db.Ping(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to ping database: %s", err)
	}
	return &healthcheckpb.ReadinessProbeResponse{}, nil
}

func (s *server) StartupProbe(context.Context, *healthcheckpb.StartupProbeRequest) (*healthcheckpb.StartupProbeResponse, error) {
	if err := s.db.Ping(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to ping database: %s", err)
	}
	return &healthcheckpb.StartupProbeResponse{}, nil
}
