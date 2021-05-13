/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/27 下午4:50 _*_
**/

package main

import (
	"errors"
	"fmt"
	"io"
	"move_sectors/mv_utils"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	StatusOnWaiting = "StatusOnWaiting"
	StatusOnWorking = "StatusOnWorking"
	StatusDone      = "StatusDone"
	ProofType32G    = "32G"
	ProofType64G    = "64G"
	TreeRFormat     = "sc-02-data-tree-r-last-%d.dat"
)

type ComputersMap struct {
	CMap  map[string]Computer
	CLock *sync.Mutex
}

type TaskList struct {
	Ops   []Operation
	TLock *sync.Mutex
}

type Operation interface {
	printInfo()
	canDo() bool
	getBestDst(singlePathThreadLimit int) (string, string, int, error)
	startCopy(cfg *Config, dstPathIdxInComp int)
	releaseSrcComputer()
	releaseDstComputer()
	getStatus() string
	setStatus(st string)
	fullInfo(dstOri, dstIp string)
	occupyDstPathThread(idx int, c *Computer)
	freeDstPathThread(idx int)
	makeDstPathSliceForCheckIsCopied(oriDst string) ([]string, error)
	checkIsCopied(cfg *Config) bool
}

func getOneFreeDstComputer() (*Computer, error) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	for _, com := range dstComputersMapSingleton.CMap {
		if com.CurrentThreads < com.LimitThread {
			com.CurrentThreads++
			dstComputersMapSingleton.CMap[com.Ip] = com
			return &com, nil
		}
	}
	return nil, errors.New("no free dst computers for now")
}

func copyDir(srcDir, dst string, cfg *Config) error {
	if err := mv_utils.MakeDirIfNotExists(dst); err != nil {
		return err
	}
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if stop {
			return nil
		}
		if info == nil || err != nil {
			return err
		}
		if path != srcDir {
			err = copying(path, dst+"/"+info.Name(), 0, cfg.Chunks)
		}
		return err
	})
	return err
}

func copying(src, dst string, singleThreadMBPS int, chunks int64) (err error) {
	statSrc, err := os.Stat(src)
	if err != nil {
		return err
	}
	statDst, err := os.Stat(dst)
	if err == nil {
		if statDst.Size() == statSrc.Size() {
			srcHash, _ := mv_utils.CalFileHash(src, statSrc.Size(), chunks)
			dstHash, _ := mv_utils.CalFileHash(dst, statDst.Size(), chunks)
			now := time.Now()
			if srcHash == dstHash && srcHash != "" && dstHash != "" {
				if os.Getenv("SHOW_LOG_DETAIL") == "1" {
					log.Infof("src file: %s already existed in dst %s,CacheSealedTask done,calHash cost %v", src, dst, time.Now().Sub(now))
				}
				return nil
			}
		}
	}

	const BufferSize = 1 * 1024 * 1024
	buf := make([]byte, BufferSize)

	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		err2 := source.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}()

	err = mv_utils.MakeDirIfNotExists(path.Dir(dst))
	if err != nil {
		return err
	}
	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		err2 := destination.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}()
	readed := 0
	for {
		if stop {
			return errors.New("stop by syscall")
		}

		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// 限速
		readed += len(buf)
		if singleThreadMBPS != 0 {
			if readed >= (singleThreadMBPS << 20) {
				readed = 0
				time.Sleep(time.Second * 1)
			}
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
	}
	return
}

func checkAndFindCacheSrc(cacheSrcDir, oriSrc string) string {
	// verify all src files existed, if not existed, find all computers
	var needFindCacheSrcDir bool
	if _, errCacheSrcDir := os.Stat(cacheSrcDir); errCacheSrcDir != nil && os.IsNotExist(errCacheSrcDir) {
		cacheSrcDir = ""
		needFindCacheSrcDir = true
	}
	if needFindCacheSrcDir {
		var errFind error
	FindCacheSrcLoopCache:
		for _, comp := range srcComputersMapSingleton.CMap {
			for _, singlePath := range comp.Paths {
				cacheSrcDirTmp := strings.Replace(cacheSrcDir, oriSrc, strings.TrimRight(singlePath.Location, "/"), 1)
				if _, errFind = os.Stat(cacheSrcDirTmp); errFind == nil {
					cacheSrcDir = cacheSrcDirTmp
					break FindCacheSrcLoopCache
				}
			}
		}
	}
	return cacheSrcDir
}
