package server

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/options"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
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

type Server struct {
	dbs        []*bitcask.BitcaskDB //
	serverOpts *ServerOptions
}

func init() {
	path := "../resource/banner.txt"
	banner, _ := ioutil.ReadFile(path)
	fmt.Println(string(banner))
}

func NewServer(opt *ServerOptions) *Server {
	return &Server{
		dbs:        openAllDB(opt),
		serverOpts: opt,
	}
}

var once sync.Once
var defaultServer *Server
var DefaultOption = &ServerOptions{
	dbPath:    filepath.Join("/tmp", "bitcaskDB"),
	host:      "127.0.0.1",
	port:      "55201",
	databases: 16,
}

func DefaultServer() *Server {
	once.Do(func() {
		defaultServer = NewServer(DefaultOption)
	})
	return defaultServer
}

func openAllDB(opt *ServerOptions) (dbs []*bitcask.BitcaskDB) {
	// open databases and choose the first db as default db
	now := time.Now()
	for i := uint(0); i < opt.databases; i++ {
		path := filepath.Join(opt.dbPath, fmt.Sprintf(dbName, i))
		opts := options.DefaultOptions(path)
		db, err := bitcask.Open(opts)
		if err != nil {
			log.Errorf("open db err, fail to start server. %v", err)
			return
		}
		dbs = append(dbs, db)
	}
	log.Infof("open db from [%s] successfully, time cost: %v", opt.dbPath, time.Since(now))
	return dbs
}

func (srv *Server) Start() {
	// quit signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// start listening
	listener, err := net.Listen("tcp", srv.serverOpts.host+":"+srv.serverOpts.port)
	if err != nil {
		log.Errorf("listener err: %v", err)
	}
	log.Infof("start listen at %v", listener.Addr().String())

	go srv.listen(listener)

	defer func() {
		if err := listener.Close(); err != nil {
			log.Errorf("listener close err : [%v]", err)
		}

		for _, db := range srv.dbs {
			if err := db.Close(); err != nil {
				log.Errorf("close db err : %v", err)
			}
		}
		log.Info("close server success...")
	}()

	<-sig // Wait for quitting.
}

func (srv *Server) listen(listener net.Listener) {
	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Errorf("accept err : [%v]", err)
		}
		log.Infof("new conn : [%v]", conn.LocalAddr())

		clientHandle := NewClientHandle(conn, srv.dbs[0], srv.dbs)

		go clientHandle.Handle()
	}
}
