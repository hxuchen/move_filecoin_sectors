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
	"move_sectors/mv_utils"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type CacheSealedTask struct {
	SectorID      string
	SrcIp         string
	OriSrc        string
	DstIp         string
	CacheSrcDir   string
	SealedSrc     string
	CacheDstDir   string
	SealedDst     string
	TotalSize     int64
	Status        string
	SealProofType string
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
	if sealedSrcInfo.Size() >= (34359738368-16<<10) && sealedSrcInfo.Size() <= (34359738368+16<<10) {
		task.SealProofType = ProofType32G
	} else if sealedSrcInfo.Size() >= (68719476736-16<<10) && sealedSrcInfo.Size() <= (68719476736+16<<10) {
		task.SealProofType = ProofType64G
	} else {
		return task, errors.New("task's sealed file size not 32G or 64G,we can not deal it now")
	}
	task.SectorID = sealedId
	task.SrcIp = srcIP
	task.OriSrc = oriSrc
	task.CacheSrcDir = cacheSrcDir
	task.SealedSrc = sealedSrc
	task.TotalSize = totalSize
	task.Status = StatusOnWaiting
	return task, nil
}

func (t *CacheSealedTask) getBestDst() (string, string, int, error) {
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
		if stat.Bavail*uint64(stat.Bsize) > uint64(t.TotalSize) && p.CurrentThreads < p.SinglePathThreadLimit {
			t.occupyDstPathThread(idx, dstC)
			return p.Location, dstC.Ip, idx, nil
		}
	}
	return "", "", 0, errors.New(move_common.NoDstSuitableForNow)
}

func (t *CacheSealedTask) canDo() bool {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.SrcIp]
	if srcComputer.CurrentThreads < srcComputer.LimitThread {
		srcComputer.CurrentThreads++
		srcComputersMapSingleton.CMap[t.SrcIp] = srcComputer
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
	srcComputer := srcComputersMapSingleton.CMap[t.SrcIp]
	srcComputer.CurrentThreads--
	srcComputersMapSingleton.CMap[t.SrcIp] = srcComputer
}

func (t *CacheSealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.DstIp]
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComputer
}

func (t *CacheSealedTask) getStatus() string {
	return t.Status
}

func (t *CacheSealedTask) setStatus(st string) {
	t.Status = st
}

func (t *CacheSealedTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	log.Infof("start tp copying %v", *t)
	// copying cache
	err := copyDir(t.CacheSrcDir, t.CacheDstDir, cfg)
	if err != nil {
		log.Error(err)
		taskListSingleton.TLock.Lock()
		t.setStatus(StatusOnWaiting)
		taskListSingleton.TLock.Unlock()
		t.releaseSrcComputer()
		t.releaseDstComputer()
		t.freeDstPathThread(dstPathIdxInComp)
		os.RemoveAll(t.CacheDstDir)
		return
	}
	// copying sealed
	err = copying(t.SealedSrc, t.SealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		taskListSingleton.TLock.Lock()
		t.setStatus(StatusOnWaiting)
		taskListSingleton.TLock.Unlock()
		log.Error(err)
		t.releaseSrcComputer()
		t.releaseDstComputer()
		t.freeDstPathThread(dstPathIdxInComp)
		os.Remove(t.SealedDst)
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
	t.CacheDstDir = strings.Replace(t.CacheSrcDir, t.OriSrc, dstOri, 1)
	t.SealedDst = strings.Replace(t.SealedSrc, t.OriSrc, dstOri, 1)
	t.DstIp = dstIp
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
	dstComp := dstComputersMapSingleton.CMap[t.DstIp]
	dstComp.Paths[idx].CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComp
}

func (t *CacheSealedTask) makeDstPathSliceForCheckIsCopied(oriDst string) ([]string, error) {
	paths := make([]string, 0)
	var TreeRNum int
	switch t.SealProofType {
	case ProofType32G:
		TreeRNum = 8
	case ProofType64G:
		TreeRNum = 16
	default:
		return paths, errors.New(fmt.Sprintf("wrong file task SealProofType: %d", t.SealProofType))
	}

	var cacheDir string
	var sealedPath string

	if oriDst == "" {
		cacheDir = t.CacheSrcDir
		sealedPath = t.SealedSrc
	} else {
		cacheDir = strings.Replace(t.CacheSrcDir, t.OriSrc, oriDst, 1)
		sealedPath = strings.Replace(t.SealedSrc, t.OriSrc, oriDst, 1)
	}

	paths = append(paths,
		path.Join(cacheDir, "t_aux"),
		path.Join(cacheDir, "p_aux"),
	)
	for i := 0; i < TreeRNum; i++ {
		paths = append(paths, path.Join(cacheDir, fmt.Sprintf(TreeRFormat, i)))
	}
	paths = append(paths, sealedPath)
	return paths, nil
}

func (t *CacheSealedTask) checkIsCopied(cfg *Config) bool {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()

	for _, v := range dstComputersMapSingleton.CMap {
		for _, p := range v.Paths {
			filePaths, err := t.makeDstPathSliceForCheckIsCopied(p.Location)
			if err != nil {
				log.Error(err)
			}
			tag := 1
			for _, singleFilePath := range filePaths {
				src := strings.Replace(singleFilePath, p.Location, t.OriSrc, 1)
				statSrc, _ := os.Stat(src)
				statDst, err := os.Stat(singleFilePath)
				// if existed,check hash
				if err == nil {
					if statDst.Size() == statSrc.Size() {
						srcHash, _ := mv_utils.CalFileHash(src, statSrc.Size(), cfg.Chunks)
						dstHash, _ := mv_utils.CalFileHash(singleFilePath, statDst.Size(), cfg.Chunks)
						if srcHash == dstHash && srcHash != "" && dstHash != "" {
							tag = tag * 1
						} else {
							tag = tag * 0
						}
					}
				}
			}
			if tag == 1 {
				return true
			}
		}
	}
	return false
}
