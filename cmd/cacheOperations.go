/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/5/21 上午9:47 _*_
**/

package main

import (
	"errors"
	"fmt"
	"move_sectors/move_common"
	"move_sectors/mv_utils"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type CacheTask struct {
	SectorID      string
	SrcIp         string
	OriSrc        string
	CacheSrcDir   string
	DstIp         string
	CacheDstDir   string
	TotalSize     int64
	Status        string
	SealProofType string
}

func newCacheTask(singleCacheSrcDir, sealedId, oriSrc, srcIP string) (*CacheTask, error) {
	var task = new(CacheTask)
	// cal total cache size
	var totalSize int64
	_ = filepath.Walk(singleCacheSrcDir, func(path string, info os.FileInfo, err error) error {
		totalSize += info.Size()
		return nil
	})

	// will splice the cache file path slice after according to sector size
	if totalSize >= 73<<20 && totalSize <= 74<<20 {
		task.SealProofType = ProofType32G
	} else if totalSize >= 146<<20 && totalSize <= 147<<20 {
		task.SealProofType = ProofType64G
	} else {
		log.Warnf("sector file cache size of %s is not 32G or 64G,we can not deal it now", sealedId)
		return nil, nil
	}
	oriSrc = strings.TrimRight(oriSrc, "/")
	task.SectorID = sealedId
	task.SrcIp = srcIP
	task.OriSrc = oriSrc
	task.CacheSrcDir = singleCacheSrcDir
	task.TotalSize = totalSize
	task.Status = StatusOnWaiting
	return task, nil
}

func (t *CacheTask) getBestDst() (string, string, int, error) {

	return "", "", 0, errors.New(move_common.NoDstSuitableForNow)
}

func (t *CacheTask) canDo() bool {

	return false
}

func (t *CacheTask) printInfo() {
	fmt.Println(*t)
}

func (t *CacheTask) releaseSrcComputer() {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.SrcIp]
	srcComputer.CurrentThreads--
	srcComputersMapSingleton.CMap[t.SrcIp] = srcComputer
}

func (t *CacheTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.DstIp]
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComputer
}

func (t *CacheTask) getStatus() string {
	return t.Status
}

func (t *CacheTask) setStatus(st string) {
	t.Status = st
}

func (t *CacheTask) startCopy(cfg *Config, dstPathIdxInComp int) {}

func (t *CacheTask) fullInfo(dstOri, dstIp string) {

}

func (t *CacheTask) occupyDstPathThread(idx int, c *Computer) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	c.Paths[idx].CurrentThreads++
	dstComputersMapSingleton.CMap[c.Ip] = *c
}

func (t *CacheTask) freeDstPathThread(idx int) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComp := dstComputersMapSingleton.CMap[t.DstIp]
	dstComp.Paths[idx].CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComp
}

func (t *CacheTask) makeDstPathSliceForCheckIsCopied(oriDst string) ([]string, error) {
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
	} else {
		cacheDir = strings.Replace(t.CacheSrcDir, t.OriSrc, oriDst, 1)
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

func (t *CacheTask) checkIsCopied(cfg *Config) bool {
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
				} else {
					tag = tag * 0
				}
			}
			if tag == 1 {
				return true
			}
		}
	}
	return false
}
