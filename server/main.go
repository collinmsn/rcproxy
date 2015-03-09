package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/artyom/autoflags"
	"github.com/collinmsn/rcproxy/proxy"
	"os"
	"os/signal"
	"strings"
	"time"
)

var config = struct {
	//flag:"flagName,usage string"
	Port                   int           `flag:"port, proxy serving port"`
	StartupNodes           string        `flag:"startup-nodes, startup nodes used to query cluster topology"`
	ConnectTimeout         time.Duration `flag:"connect-timeout, connect to backend timeout"`
	ReadTimeout            time.Duration `flag:"read-timeout, read from backend timeout"`
	SlotsReloadInterval    time.Duration `flag:"slots-reload-interval, slots reload interval"`
	LogLevel               string        `flag:"log-level, log level eg. debug, info, warn, error, fatal and panic"`
	BackendIdleConnections int           `flag:"backend-idle-connections, max number of idle connections for each backend server"`
}{
	Port:                   8088,
	StartupNodes:           "10.4.17.164:7001",
	ConnectTimeout:         1 * time.Second,
	ReadTimeout:            1 * time.Second,
	SlotsReloadInterval:    3 * time.Second,
	LogLevel:               "info",
	BackendIdleConnections: 5,
}

func main() {
	if err := autoflags.Define(&config); err != nil {
		log.Fatal(err)
	}
	flag.Parse()
	if level, err := log.ParseLevel(config.LogLevel); err != nil {
		log.Fatal(err)
	} else {
		log.SetLevel(level)
	}
	log.Infof("%#v", config)
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	startupNodes := strings.Split(config.StartupNodes, ",")
	connPool := proxy.NewConnPool(config.BackendIdleConnections, config.ConnectTimeout)
	slotTable := proxy.NewSlotTable(startupNodes, connPool)
	if err := slotTable.Init(); err != nil {
		log.Fatal(err)
	} else {
		log.Infof("init slot table successfully")
	}
	proxy := proxy.NewProxy(config.Port, config.ReadTimeout, slotTable, connPool)
	go proxy.Run()
	sig := <-sigChan
	log.Infof("terminated by %#v", sig)
	slotTable.Exit()
	proxy.Exit()
}
