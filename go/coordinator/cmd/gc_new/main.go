package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chroma/chroma-coordinator/internal/gc"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dao"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func main() {
	dBConfig := dbcore.DBConfig{
		Username: "liquanpei",
		Password: "",
		Address:  "localhost",
		Port:     5432,
		DBName:   "dev",
	}
	_, err := dbcore.Connect(dBConfig)
	if err != nil {
		log.Fatal("fail to connect db", zap.Error(err))
	}

	gcInterval := 10 * time.Second
	parallelism := 5
	store := gc.NewMemoryJobStateStore()
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	inputStore := gc.NewIOStore(metaDomain, txnImpl)
	gcProcessor := gc.NewSimpleGCProcessor(inputStore, inputStore, store, gcInterval, parallelism)
	gcProcessor.Start()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	sig := <-c
	log.Info("Received signal, exiting", zap.String(sig.String(), "signal"))
}
