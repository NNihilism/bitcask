package main

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/options"
	"bitcaskDB/internal/server"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var (
	defaultDBPath            = filepath.Join("/tmp", "bitcaskDB")
	defaultHost              = "127.0.0.1"
	defaultPort              = "5200"
	defaultDatabasesNum uint = 16
)

var (
	dbs        []*bitcask.BitcaskDB //
	serverOpts *ServerOptions
)

const (
	dbName = "bitcaskDB-%04d"
)

type ServerOptions struct {
	dbPath    string
	host      string
	port      string
	databases uint
}

func main() {
	// init server options
	serverOpts = new(ServerOptions)
	flag.StringVar(&serverOpts.dbPath, "dbpath", defaultDBPath, "db path")
	flag.StringVar(&serverOpts.host, "host", defaultHost, "server host")
	flag.StringVar(&serverOpts.port, "port", defaultPort, "server port")
	flag.UintVar(&serverOpts.databases, "databases", defaultDatabasesNum, "the number of databases")
	flag.Parse()

	// open a default database
	path := filepath.Join(serverOpts.dbPath, fmt.Sprintf(dbName, 0))
	opts := options.DefaultOptions(path)
	now := time.Now()
	db, err := bitcask.Open(opts)
	if err != nil {
		log.Errorf("open db err, fail to start server. %v", err)
		return
	}
	log.Infof("open db from [%s] successfully, time cost: %v", serverOpts.dbPath, time.Since(now))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGKILL, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	listener, err := net.Listen("tcp", serverOpts.host+":"+serverOpts.port)
	// listener, err := net.Listen("tcp", ":0")

	if err != nil {
		log.Errorf("listene err: %v", err)
	}
	log.Infof("start listen at %v", listener.Addr().String())

	go listen(listener, db)

	defer func() {
		listener.Close()
		for _, db := range dbs {
			if err := db.Close(); err != nil {
				log.Errorf("close db err : %v", err)
			}
		}
		log.Info("close bitcaskDB success...")
	}()

	<-sig
	fmt.Println("after sig....")
}

func listen(listener net.Listener, defaultDB *bitcask.BitcaskDB) {
	for {
		log.Info("listen....1")
		conn, err := listener.Accept()
		log.Info("listen....2")

		if err != nil {
			log.Errorf("accept err : %v", err)
		}
		log.Infof("new conn : %v", conn)

		clientHandle := server.NewClientHandle(conn, defaultDB)
		go clientHandle.Handle()
	}
}
