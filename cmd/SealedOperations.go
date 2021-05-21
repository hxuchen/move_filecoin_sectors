/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/26 上午11:50 _*_
**/

package main

import (
	"errors"
	"github.com/filecoin-project/go-state-types/big"
	"move_sectors/move_common"
	"move_sectors/mv_utils"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
)

type SealedTask struct {
	SectorID      string
	SrcIp         string
	OriSrc        string
	SealedSrc     string
	DstIp         string
	CacheDstDir   string
	SealedDst     string
	TotalSize     int64
	Status        string
	SealProofType string
}

func newSealedTask(sealedSrc, sealedId, oriSrc, srcIP string) (*SealedTask, error) {
	var task = new(SealedTask)
	oriSrc = strings.TrimRight(oriSrc, "/")

	// check sealed file size is valid or not
	sealedSrcInfo, _ := os.Stat(sealedSrc)
	totalSize := sealedSrcInfo.Size()
	if totalSize >= (34359738368-16<<10) && totalSize <= (34359738368+16<<10) {
		task.SealProofType = ProofType32G
	} else if totalSize >= (68719476736-16<<10) && totalSize <= (68719476736+16<<10) {
		task.SealProofType = ProofType64G
	} else {
		log.Warnf("sector file sealed size of %s is not 32G or 64G,we can not deal it now", sealedId)
		return nil, nil
	}

	task.SectorID = sealedId
	task.SrcIp = srcIP
	task.OriSrc = oriSrc
	task.SealedSrc = sealedSrc
	task.TotalSize = totalSize
	task.Status = StatusOnWaiting
	return task, nil
}

func (t *SealedTask) getBestDst() (string, string, int, error) {
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

func (t *SealedTask) canDo() bool {
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

func (t *SealedTask) getInfo() interface{} {
	return *t
}

func (t *SealedTask) releaseSrcComputer() {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.SrcIp]
	srcComputer.CurrentThreads--
	srcComputersMapSingleton.CMap[t.SrcIp] = srcComputer
}

func (t *SealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.DstIp]
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComputer
}

func (t *SealedTask) getStatus() string {
	return t.Status
}

func (t *SealedTask) setStatus(st string) {
	t.Status = st
}

func (t *SealedTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	log.Infof("start tp copying %v", *t)
	// copying sealed
	err := copying(t.SealedSrc, t.SealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		if err.Error() == move_common.StoppedBySyscall {
			log.Warn(err)
		} else {
			log.Error(err)
		}
		t.releaseSrcComputer()
		t.releaseDstComputer()
		t.freeDstPathThread(dstPathIdxInComp)
		os.Remove(t.SealedDst)
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

func (t *SealedTask) fullInfo(dstOri, dstIp string) {
	//t.CacheDstDir = strings.Replace(t.CacheSrcDir, t.OriSrc, dstOri, 1)
	t.SealedDst = strings.Replace(t.SealedSrc, t.OriSrc, dstOri, 1)
	t.DstIp = dstIp
}

func (t *SealedTask) occupyDstPathThread(idx int, c *Computer) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	c.Paths[idx].CurrentThreads++
	dstComputersMapSingleton.CMap[c.Ip] = *c
}

func (t *SealedTask) freeDstPathThread(idx int) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComp := dstComputersMapSingleton.CMap[t.DstIp]
	dstComp.Paths[idx].CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComp
}

func (t *SealedTask) checkSourceSize() ([]string, error) {
	var paths = make([]string, 0)

	size, err := getStandSize(t.SealProofType, t.SealedSrc)
	if err != nil {
		return paths, err
	}
	err = compareSize(t.SealedSrc, size, 16<<10)
	if err != nil {
		return paths, err
	}

	paths = append(paths, t.SealedSrc)
	return paths, nil
}

func (t *SealedTask) checkIsExistedInDst(srcPaths []string, cfg *Config) bool {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	sinceTime := time.Now()
	for _, v := range dstComputersMapSingleton.CMap {
		for _, p := range v.Paths {
			tag := 1
			for _, singleSealedPath := range srcPaths {
				dst := strings.Replace(singleSealedPath, t.OriSrc, p.Location, 1)
				statSrc, _ := os.Stat(singleSealedPath)
				statDst, err := os.Stat(dst)
				// if existed,check hash
				if err == nil {
					if statDst.Size() == statSrc.Size() {
						srcHash, _ := recordCalLogIfNeed(mv_utils.CalFileHash, singleSealedPath, statSrc.Size(), cfg.Chunks)
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
					log.Debugf("src sealed file: %v already existed in dst %s,SealedTask done,check cost %v",
						*t, p.Location, time.Now().Sub(sinceTime))
					log.Debugf("task %v is existed in dst", *t)
				}
				return true
			}
		}
	}
	return false
}

func (t *SealedTask) tryToFindGroupDir() (string, string, int, error) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()

	// search cache at first
	for _, cmp := range dstComputersMapSingleton.CMap {
		for idx, p := range cmp.Paths {
			dstCache := strings.TrimRight(p.Location, "/") + "/cache/" + t.SectorID
			_, err := os.Stat(dstCache)
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
