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
		log.Warnf("sector file sealed size of %s is not 32G or 64G,we can not deal it now", sealedSrc)
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

func (t *SealedTask) canDo() bool {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.SrcIp]
	var pathCurrentThread int64
	var pathLimitThread int64
	for _, loc := range srcComputer.Paths {
		if t.OriSrc == loc.Location {
			pathCurrentThread = loc.CurrentThreads
			pathLimitThread = loc.SinglePathThreadLimit
		}
	}
	if srcComputer.CurrentThreads < srcComputer.LimitThread && pathCurrentThread < pathLimitThread {
		return true
	}
	return false
}

func (t *SealedTask) getBestDst() (string, string, error) {
	log.Debugf("finding best dst, %s", t.SectorID)

	dir, s, err := t.tryToFindGroupDir()
	if err != nil {
		if err.Error() == move_common.FondGroupButTooMuchThread {
			return "", "", err
		}
		dstC, err := getOneFreeDstComputer()
		if err != nil {
			return "", "", err
		}

		log.Debugf("sorting dst paths")
		paths := dstC.Paths
		sort.Slice(paths, func(i, j int) bool {
			var statI = new(syscall.Statfs_t)
			_ = syscall.Statfs(paths[i].Location, statI)
			var statJ = new(syscall.Statfs_t)
			_ = syscall.Statfs(paths[j].Location, statJ)

			iw := big.NewInt(int64(statI.Bavail*uint64(statI.Bsize)) / (paths[i].CurrentThreads + 1))
			jw := big.NewInt(int64(statJ.Bavail*uint64(statJ.Bsize)) / (paths[j].CurrentThreads + 1))

			return iw.GreaterThanEqual(jw)
		})
		log.Debugf("selecting dst paths for %s", t.SectorID)
		for _, p := range paths {
			var stat = new(syscall.Statfs_t)
			_ = syscall.Statfs(p.Location, stat)
			if stat.Bavail*uint64(stat.Bsize) > uint64(t.TotalSize) && p.CurrentThreads < p.SinglePathThreadLimit {
				return p.Location, dstC.Ip, nil
			}
		}
		log.Debugf("found group path for %s sealed", t.SectorID)
		return "", "", errors.New(move_common.NoDstSuitableForNow)
	}

	return dir, s, nil
}

func (t *SealedTask) fullInfo(dstOri, dstIp string) {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	t.SealedDst = strings.Replace(t.SealedSrc, t.OriSrc, strings.TrimRight(dstOri, "/"), 1)
	t.DstIp = dstIp
}

func (t *SealedTask) startCopy(cfg *Config, dstPath string) {
	log.Infof("start to copying %v", *t)
	// copying sealed
	err := copying(t.SealedSrc, t.SealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	freeThreads(dstPath, t.DstIp, t.SrcIp, t.OriSrc)
	if err != nil {
		if err.Error() == move_common.StoppedBySyscall {
			log.Warn(err)
		} else {
			log.Error(err)
		}
		os.Remove(t.SealedDst)
		t.setStatus(StatusOnWaiting)
	} else {
		t.setStatus(StatusDone)
		log.Infof("task %v done", *t)
	}
}

func (t *SealedTask) tryToFindGroupDir() (string, string, error) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	log.Debugf("trying to find group dir for %s sealed", t.SectorID)
	// search cache at first
	for _, cmp := range dstComputersMapSingleton.CMap {
		for _, p := range cmp.Paths {
			dstCache := strings.TrimRight(p.Location, "/") + "/cache/" + t.SectorID
			_, err := os.Stat(dstCache)
			if err == nil {
				if cmp.CurrentThreads < cmp.LimitThread && p.CurrentThreads < p.SinglePathThreadLimit {
					var stat = new(syscall.Statfs_t)
					_ = syscall.Statfs(p.Location, stat)
					if stat.Bavail*uint64(stat.Bsize) <= uint64(t.TotalSize) {
						log.Debugf("%v fond same group dir on %s, but disk has not enough space, will chose new dst", *t, p.Location)
						return "", "", errors.New(move_common.NotEnoughSpace)
					}
					log.Debug(dstComputersMapSingleton)
					return p.Location, cmp.Ip, nil
				} else {
					log.Debugf("%v fond same group dir on %s, but too much threads for now, will copy later", *t, p.Location)
					return "", "", errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	// search unSealed
	for _, cmp := range dstComputersMapSingleton.CMap {
		for _, p := range cmp.Paths {
			dstUnSealed := strings.TrimRight(p.Location, "/") + "/unsealed/" + t.SectorID
			_, err := os.Stat(dstUnSealed)
			if err == nil {
				if cmp.CurrentThreads < cmp.LimitThread && p.CurrentThreads < p.SinglePathThreadLimit {

					var stat = new(syscall.Statfs_t)
					_ = syscall.Statfs(p.Location, stat)
					if stat.Bavail*uint64(stat.Bsize) <= uint64(t.TotalSize) {
						log.Debugf("%v fond same group dir on %s, but disk has not enough space, will chose new dst", *t, p.Location)
						return "", "", errors.New(move_common.NotEnoughSpace)
					}

					return p.Location, cmp.Ip, nil
				} else {
					log.Infof("%v fond same group dir on %s, but too much threads for now, will copy later", *t, p.Location)
					return "", "", errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	return "", "", errors.New("no same group dir")
}

func (t *SealedTask) getInfo() interface{} {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return *t
}

func (t *SealedTask) getStatus() string {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return t.Status
}

func (t *SealedTask) setStatus(st string) {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	t.Status = st
}

func (t *SealedTask) getSrcIp() string {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return t.SrcIp
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
				log.Debugf("src sealed file: %v already existed in dst %s,SealedTask done,check cost %v",
					*t, p.Location, time.Now().Sub(sinceTime))
				log.Debugf("task %v is existed in dst", *t)
				return true
			}
		}
	}
	return false
}

func (t *SealedTask) getSrcPath() string {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return t.OriSrc
}
