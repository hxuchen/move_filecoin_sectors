/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/5/21 上午9:47 _*_
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
	"time"
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
	dir, s, i, err := t.tryToFindGroupDir()
	if err != nil {
		if err.Error() == move_common.FondGroupButTooMuchThread {
			return "", "", 0, err
		}
		dstComputersMapSingleton.CLock.Lock()
		defer dstComputersMapSingleton.CLock.Unlock()
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

	return dir, s, i, nil
}

func (t *CacheTask) canDo() bool {
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

func (t *CacheTask) getInfo() interface{} {
	return *t
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

func (t *CacheTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	log.Infof("start tp copying %v", *t)
	// copying cache
	err := copyDir(t.CacheSrcDir, t.CacheDstDir, cfg)
	if err != nil {
		if err.Error() == move_common.StoppedBySyscall {
			log.Warn(err)
		} else {
			log.Error(err)
		}
		t.releaseSrcComputer()
		t.releaseDstComputer()
		t.freeDstPathThread(dstPathIdxInComp)
		os.RemoveAll(t.CacheDstDir)
		taskListSingleton.TLock.Lock()
		t.setStatus(StatusOnWaiting)
		taskListSingleton.TLock.Unlock()
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

func (t *CacheTask) fullInfo(dstOri, dstIp string) {
	t.CacheDstDir = strings.Replace(t.CacheSrcDir, t.OriSrc, dstOri, 1)
	t.DstIp = dstIp
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

func (t *CacheTask) makeSrcPathSliceForCache() ([]string, error) {
	paths := make([]string, 0)
	var TreeRNum int
	switch t.SealProofType {
	case ProofType32G:
		TreeRNum = 8
	case ProofType64G:
		TreeRNum = 16
	default:
		return paths, errors.New(fmt.Sprintf("wrong file task SealProofType: %s", t.SealProofType))
	}

	paths = append(paths,
		path.Join(t.CacheSrcDir, "t_aux"),
		path.Join(t.CacheSrcDir, "p_aux"),
	)
	for i := 0; i < TreeRNum; i++ {
		paths = append(paths, path.Join(t.CacheSrcDir, fmt.Sprintf(TreeRFormat, i)))
	}

	return paths, nil
}

func (t *CacheTask) checkIsExistedInDst(srcPaths []string, cfg *Config) bool {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	sinceTime := time.Now()
	for _, v := range dstComputersMapSingleton.CMap {
		for _, p := range v.Paths {
			tag := 1
			for _, singleCachePath := range srcPaths {
				dst := strings.Replace(singleCachePath, t.OriSrc, p.Location, 1)
				statSrc, _ := os.Stat(singleCachePath)
				statDst, err := os.Stat(dst)
				// if existed,check hash
				if err == nil {
					if statDst.Size() == statSrc.Size() {
						srcHash, _ := recordCalLogIfNeed(mv_utils.CalFileHash, singleCachePath, statSrc.Size(), cfg.Chunks)
						dstHash, _ := recordCalLogIfNeed(mv_utils.CalFileHash, dst, statDst.Size(), cfg.Chunks)
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
				if showLogDetail {
					log.Debugf("src cache file: %v already existed in dst %s,SealedTask done,check cost %v",
						*t, p.Location, time.Now().Sub(sinceTime))
					log.Debugf("task %v is existed in dst", *t)
				}
				return true
			}
		}
	}
	return false
}

func (t *CacheTask) checkSourceSize() ([]string, error) {
	paths, err := t.makeSrcPathSliceForCache()
	if err != nil {
		return paths, err
	}
	if len(paths) == 0 {
		return paths, errors.New("wrong path slice size")
	} else {
		for _, p := range paths {
			if strings.Contains(p, "t_aux") {
				if fileStat, err := os.Stat(p); err != nil {
					return paths, err
				} else {
					if fileStat.Size() == 0 {
						return paths, errors.New(fmt.Sprintf("wrong file size,path: %s, got size: %d", p, fileStat.Size()))
					}
					continue
				}
			}
			size, err := getStandSize(t.SealProofType, p)
			if err != nil {
				return paths, err
			}
			err = compareSize(p, size, 16<<10)
			if err != nil {
				return paths, err
			}
		}
	}
	return paths, nil
}

func (t *CacheTask) tryToFindGroupDir() (string, string, int, error) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()

	// search sealed at first
	for _, cmp := range dstComputersMapSingleton.CMap {
		for idx, p := range cmp.Paths {
			dstSealed := strings.TrimRight(p.Location, "/") + "/sealed/" + t.SectorID
			_, err := os.Stat(dstSealed)
			if err == nil {
				if cmp.CurrentThreads < cmp.LimitThread {
					return p.Location, cmp.Ip, idx, nil
				} else {
					if showLogDetail {
						log.Debugf("%v fond same group dir on %s, but computer too much, will copy later", *t, p.Location)
					}
					return "", "", 0, errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	// search unSealed
	for _, cmp := range dstComputersMapSingleton.CMap {
		for idx, p := range cmp.Paths {
			dstUnSealed := strings.TrimRight(p.Location, "/") + "/unsealed/" + t.SectorID
			_, err := os.Stat(dstUnSealed)
			if err == nil {
				if cmp.CurrentThreads < cmp.LimitThread {
					return p.Location, cmp.Ip, idx, nil
				} else {
					if showLogDetail {
						log.Infof("%v fond same group dir on %s, but computer too much, will copy later", *t, p.Location)
					}
					return "", "", 0, errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	return "", "", 0, errors.New("no same group dir")
}
