package main

import (
	"github.com/filecoin-project/lotus/lib/lotuslog"
	logging "github.com/ipfs/go-log"
	"github.com/urfave/cli/v2"
	"move_sectors/build"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	log                   = logging.Logger("main")
	computersMapSingleton = ComputersMap{
		CMap:  make(map[string]Computer),
		CLock: new(sync.Mutex),
	}
	stop = false
)

/*
	cmd include
		--srcPath special the file that contains the source paths
		--dstPath  transfer target location(required)
		--minerIP special the miner address
*/
func main() {
	lotuslog.SetupLogLevels()

	cmd := []*cli.Command{
		CpCmd,
	}
	app := &cli.App{
		Name:     "move-sectors",
		Usage:    "copy sectors to another location",
		Version:  build.GetVersion(),
		Commands: cmd,
		Flags:    nil,
	}
	app.Setup()
	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var CpCmd = &cli.Command{
	Name:  "run",
	Usage: "start to copy files",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "path",
			Usage:    "special the config file paths",
			Required: false,
			Hidden:   false,
			Value:    "~/mv_sectors.yaml",
		},
	},

	Action: func(cctx *cli.Context) error {
		log.Infof("start move_sector,version:%s", build.GetVersion())

		config, err := getConfig(cctx)
		if err != nil {
			log.Error(err)
			return nil
		}
		stopSignal := make(chan os.Signal, 2)
		signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			select {
			case <-stopSignal:
				stop = true
			}
		}()
		log.Info("start to copy")
		start(config)
		log.Info("mv_sectors exited")
		return nil
	},
}
