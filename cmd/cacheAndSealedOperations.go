/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/26 上午11:50 _*_
**/

package main

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/big"
	"move_sectors/move_common"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type CacheSealedTask struct {
	srcIp       string
	oriSrc      string
	dstIp       string
	cacheSrcDir string
	sealedSrc   string
	cacheDstDir string
	sealedDst   string
	totalSize   int64
	status      string
}

func newCacheSealedTask(sealedSrc, sealedId, oriSrc, srcIP string) (*CacheSealedTask, error) {
	var task = new(CacheSealedTask)
	oriSrc = strings.TrimRight(oriSrc, "/")
	cacheSrcDir := oriSrc + "/" + "cache" + "/" + sealedId

	cacheSrcDir = checkAndFindCacheSrc(cacheSrcDir, oriSrc)
	if cacheSrcDir == "" {
		return task, fmt.Errorf("%s: %s", move_common.SourceFileNotExisted, cacheSrcDir)
	}

	var totalSize int64
	_ = filepath.Walk(cacheSrcDir, func(path string, info os.FileInfo, err error) error {
		totalSize += info.Size()
		return nil
	})
	sealedSrcInfo, _ := os.Stat(sealedSrc)
	totalSize += sealedSrcInfo.Size()

	task.srcIp = srcIP
	task.oriSrc = oriSrc
	task.cacheSrcDir = cacheSrcDir
	task.sealedSrc = sealedSrc
	task.totalSize = totalSize
	task.status = StatusOnWaiting
	return task, nil
}

func (t *CacheSealedTask) getBestDst(singlePathThreadLimit int) (string, string, int, error) {
	dstC, err := getOneFreeDstComputer()
	if err != nil {
		return "", "", 0, err
	}

	sort.Slice(dstC.Paths, func(i, j int) bool {
		iw := big.NewInt(dstC.Paths[i].CurrentThreads)
		jw := big.NewInt(dstC.Paths[j].CurrentThreads)
		return iw.GreaterThanEqual(jw)
	})

	for idx, p := range dstC.Paths {
		var stat = new(syscall.Statfs_t)
		_ = syscall.Statfs(p.Location, stat)
		if stat.Bavail*uint64(stat.Bsize) > uint64(t.totalSize) && p.CurrentThreads < int64(singlePathThreadLimit) {
			t.occupyDstPathThread(idx, dstC)
			return p.Location, dstC.Ip, idx, nil
		}
	}
	return "", "", 0, errors.New(move_common.NoDstSuitableForNow)
}

func (t *CacheSealedTask) canDo() bool {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.srcIp]
	if srcComputer.CurrentThreads < srcComputer.LimitThread {
		srcComputer.CurrentThreads++
		srcComputersMapSingleton.CMap[t.srcIp] = srcComputer
		return true
	}
	return false
}

func (t *CacheSealedTask) printInfo() {
	fmt.Println(*t)
}

func (t *CacheSealedTask) releaseSrcComputer() {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.srcIp]
	srcComputer.CurrentThreads--
	srcComputersMapSingleton.CMap[t.srcIp] = srcComputer
}

func (t *CacheSealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.dstIp]
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.dstIp] = dstComputer
}

func (t *CacheSealedTask) getStatus() string {
	return t.status
}

func (t *CacheSealedTask) setStatus(st string) {
	t.status = st
}

func (t *CacheSealedTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	log.Infof("start tp copying %v", *t)
	// copying cache
	err := copyDir(t.cacheSrcDir, t.cacheDstDir, cfg)
	if err != nil {
		log.Error(err)
		taskListSingleton.TLock.Lock()
		t.setStatus(StatusOnWaiting)
		taskListSingleton.TLock.Unlock()
		t.releaseSrcComputer()
		t.releaseDstComputer()
		t.freeDstPathThread(dstPathIdxInComp)
		return
	}
	// copying sealed
	err = copying(t.sealedSrc, t.sealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		taskListSingleton.TLock.Lock()
		t.setStatus(StatusOnWaiting)
		taskListSingleton.TLock.Unlock()
		log.Error(err)
		t.releaseSrcComputer()
		t.releaseDstComputer()
		t.freeDstPathThread(dstPathIdxInComp)
		return
	}
	taskListSingleton.TLock.Lock()
	t.setStatus(StatusDone)
	taskListSingleton.TLock.Unlock()
	t.releaseSrcComputer()
	t.releaseDstComputer()
	t.freeDstPathThread(dstPathIdxInComp)
	log.Infof("task %v done", *t)
}

func (t *CacheSealedTask) fullInfo(dstOri, dstIp string) {
	t.cacheDstDir = strings.Replace(t.cacheSrcDir, t.oriSrc, dstOri, 1)
	t.sealedDst = strings.Replace(t.sealedSrc, t.oriSrc, dstOri, 1)
	t.dstIp = dstIp
}

func (t *CacheSealedTask) occupyDstPathThread(idx int, c *Computer) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	c.Paths[idx].CurrentThreads++
	dstComputersMapSingleton.CMap[c.Ip] = *c
}

func (t *CacheSealedTask) freeDstPathThread(idx int) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComp := dstComputersMapSingleton.CMap[t.dstIp]
	dstComp.Paths[idx].CurrentThreads--
	dstComputersMapSingleton.CMap[t.dstIp] = dstComp
}
