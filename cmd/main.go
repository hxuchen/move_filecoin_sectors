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
	"move_sectors/move_common"
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
	skipSourceError   = false
	fileType          move_common.FileType
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
		Usage:    "copying sectors to another location",
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
	Usage: "startWork to copying files",
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
			Usage:    "Declare whether to copying unsealed files",
			Required: false,
			Hidden:   false,
			Value:    false,
		},
		&cli.BoolFlag{
			Name:     "Sealed",
			Usage:    "Declare whether to copying Sealed files",
			Required: false,
			Hidden:   false,
			Value:    false,
		},
		&cli.BoolFlag{
			Name:     "Cache",
			Usage:    "Declare whether to copying cache files",
			Required: false,
			Hidden:   false,
			Value:    false,
		},
		&cli.BoolFlag{
			Name:     "SkipSourceError",
			Usage:    "Declare whether to copying cache files",
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

		// which kind file will be moved
		if !cctx.Bool("UnSealed") && !cctx.Bool("Sealed") && !cctx.Bool("Cache") {
			return errors.New("you must tell which kind of file to move,options: --UnSealed,--Sealed,--Cache")
		}
		if (cctx.Bool("UnSealed") && cctx.Bool("Sealed")) ||
			(cctx.Bool("UnSealed") && cctx.Bool("Cache")) ||
			(cctx.Bool("Sealed") && cctx.Bool("Cache")) {
			return errors.New("only one kind of file once")
		}
		if cctx.Bool("UnSealed") {
			fileType = move_common.UnSealed
		}
		if cctx.Bool("Sealed") {
			fileType = move_common.Sealed
		}
		if cctx.Bool("Cache") {
			fileType = move_common.Cache
		}

		if cctx.Bool("SkipSourceError") {
			skipSourceError = true
		}

		// load config
		config, err := getConfig(cctx)
		if err != nil {
			log.Error(err)
			return nil
		}
		err = initializeComputerMapSingleton(config)
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

		log.Info("startWork to copying")
		startWork(config)
		log.Info("mv_sectors exited")
		return nil
	},
}

//Check scheduler process if existed
func createFileLock(confDir, lockFileName string) (io.Closer, error) {
	locked, err := fslock.Locked(confDir, lockFileName)
	if err != nil {
		return nil, xerrors.Errorf("could not check lock Status: %w", err)
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
