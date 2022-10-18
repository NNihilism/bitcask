package main

import "bitcaskDB/internal/server"

//var (
//	defaultDBPath            = filepath.Join("/tmp", "bitcaskDB")
//	defaultHost              = "127.0.0.1"
//	defaultPort              = "55201"
//	defaultDatabasesNum uint = 16
//)
//
//var (
//	dbs        []*bitcask.BitcaskDB //
//	serverOpts *ServerOptions
//)
//
//const (
//	dbName = "bitcaskDB-%04d"
//)
//
//type ServerOptions struct {
//	dbPath    string
//	host      string
//	port      string
//	databases uint
//}
//
//func init() {
//	path := "../../internal/resource/banner.txt"
//	banner, _ := ioutil.ReadFile(path)
//	fmt.Println(string(banner))
//}

func main() {
	server.DefaultServer().Start()
}

//	// init server config
//	serverOpts = new(ServerOptions)
//	flag.StringVar(&serverOpts.dbPath, "dbpath", defaultDBPath, "db path")
//	flag.StringVar(&serverOpts.host, "host", defaultHost, "server host")
//	flag.StringVar(&serverOpts.port, "port", defaultPort, "server port")
//	flag.UintVar(&serverOpts.databases, "databases", defaultDatabasesNum, "the number of databases")
//	flag.Parse()
//
//	// open databases and choose the first db as default db
//	now := time.Now()
//	for i := uint(0); i < defaultDatabasesNum; i++ {
//		path := filepath.Join(serverOpts.dbPath, fmt.Sprintf(dbName, i))
//		opts := options.DefaultOptions(path)
//		db, err := bitcask.Open(opts)
//		if err != nil {
//			log.Errorf("open db err, fail to start server. %v", err)
//			return
//		}
//		dbs = append(dbs, db)
//	}
//	log.Infof("open db from [%s] successfully, time cost: %v", serverOpts.dbPath, time.Since(now))
//
//	// quit signal
//	sig := make(chan os.Signal, 1)
//	signal.Notify(sig, syscall.SIGTERM, syscall.SIGHUP,
//		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
//
//	// start listening
//	listener, err := net.Listen("tcp", serverOpts.host+":"+serverOpts.port)
//	if err != nil {
//		log.Errorf("listene err: %v", err)
//	}
//	log.Infof("start listen at %v", listener.Addr().String())
//
//	go listen(listener)
//
//	defer func() {
//		listener.Close()
//		for _, db := range dbs {
//			if err := db.Close(); err != nil {
//				log.Errorf("close db err : %v", err)
//			}
//		}
//		log.Info("close server success...")
//	}()
//
//	<-sig // Wait for quitting.
//}
//
//func listen(listener net.Listener) {
//	for {
//		conn, err := listener.Accept()
//
//		if err != nil {
//			log.Errorf("accept err : %v", err)
//		}
//		log.Infof("new conn : %v", conn.LocalAddr())
//
//		clientHandle := server.NewClientHandle(conn, dbs[0], dbs)
//
//		go clientHandle.Handle()
//
//	}
//}
