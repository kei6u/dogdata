package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	if err := profiler.Start(
		profiler.WithService(os.Getenv("DD_SERVICE")),
		profiler.WithProfileTypes(
			profiler.CPUProfile,
			profiler.HeapProfile,
		),
	); err != nil {
		logger.Warn("failed to start profiler", zap.Error(err))
		return
	}
	defer profiler.Stop()

	db, closeDB, err := newDB()
	if err != nil {
		logger.Warn("failed to connect to database", zap.Error(err))
		logger.Warn("exit the process....")
		return
	}
	defer closeDB()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-c
		logger.Info("Server shut down in 5 seconds")
		time.Sleep(5 * time.Second)
		cancel()
	}()

	go func() {
		s := newServer(
			os.Getenv("GRPC_ADDR"),
			os.Getenv("GRPC_GATEWAY_ADDR"),
			logger,
			db,
		)
		s.Start(ctx)
	}()

	<-ctx.Done()
}
