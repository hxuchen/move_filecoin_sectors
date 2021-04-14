package main

import (
	"encoding/ascii85"
	logging "github.com/ipfs/go-log"
	"github.com/urfave/cli/v2"
	"move_sectors/build"
	"move_sectors/mv_common"
	"move_sectors/mv_utils"
	"os"
	"strings"
)

var (
	log                   = logging.Logger("main")
	computersMapSingleton = make(map[string]Computer)
)

/*
	cmd include
		--srcPath special the file that contains the source paths
		--dstPath  transfer target location(required)
		--minerIP special the miner address
*/
func main() {
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
	Name:  "copy",
	Usage: "start to copy files",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "Path",
			Usage:    "special the config file paths",
			Required: true,
			Hidden:   false,
			Value:    "~/mv_sectors.yaml",
		},
	},

	Action: func(cctx *cli.Context) error {
		log.Info("start move_sector,version:%s", build.GetVersion())
		file, err2 := LoadConfigFromFile()

		srcPath := cctx.String("srcPath")
		totalUsage, err := initializeSrcPathList(srcPath)
		if err != nil {
			log.Error(err)
			return nil
		}

		dstPath := cctx.String("dstPath")
		availableSize, err := mv_utils.GetAvailableSize(dstPath)
		if err != nil {
			log.Error(err)
			return nil
		}

		if availableSize < totalUsage {
			log.Errorf("%s has no enough space to store all files", dstPath)
			return nil
		}

		startCopy()

	},
}
