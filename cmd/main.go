package main

import (
	"errors"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	fslock "github.com/ipfs/go-fs-lock"
	logging "github.com/ipfs/go-log"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io"
	"move_sectors/build"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	log                      = logging.Logger("main")
	srcComputersMapSingleton = ComputersMap{
		CMap:  make(map[string]Computer),
		CLock: new(sync.Mutex),
	}
	dstComputersMapSingleton = ComputersMap{
		CMap:  make(map[string]Computer),
		CLock: new(sync.Mutex),
	}
	stop              = false
	doUnSealed        = false
	taskListSingleton = TaskList{
		Ops:   make([]Operation, 0),
		TLock: new(sync.Mutex),
	}
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
	Usage: "startWork to copy files",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "path",
			Usage:    "special the config file paths",
			Required: false,
			Hidden:   false,
			Value:    "~/mv_sectors.yaml",
		},
		&cli.BoolFlag{
			Name:     "UnSealed",
			Usage:    "Declare whether to copy unsealed files only,default just copy cache and sealed",
			Required: false,
			Hidden:   false,
			Value:    false,
		},
	},

	Action: func(cctx *cli.Context) error {
		log.Infof("startWork move_sector,version:%s", build.GetVersion())
		lock, err2 := createFileLock(os.TempDir(), "move_sectors.lock")
		if err2 != nil {
			log.Error(err2)
			return err2
		}

		if lock != nil {
			defer lock.Close()
		} else {
			return errors.New("create file lock failed")
		}

		config, err := getConfig(cctx)
		if err != nil {
			log.Error(err)
			return nil
		}
		if cctx.Bool("UnSealed") {
			doUnSealed = true
		}
		stopSignal := make(chan os.Signal, 2)
		signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			select {
			case <-stopSignal:
				stop = true
			}
		}()
		log.Info("startWork to copy")
		startWork(config)
		log.Info("mv_sectors exiting")
		return nil
	},
}

//Check scheduler process if exist
func createFileLock(confDir, lockFileName string) (io.Closer, error) {
	locked, err := fslock.Locked(confDir, lockFileName)
	if err != nil {
		return nil, xerrors.Errorf("could not check lock status: %w", err)
	}
	if locked {
		return nil, errors.New("program is already running")
	}

	closer, err := fslock.Lock(confDir, lockFileName)
	if err != nil {
		return nil, xerrors.Errorf("could not lock the repo: %w", err)
	}
	return closer, nil
}
