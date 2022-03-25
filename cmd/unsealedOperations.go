/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/27 下午4:49 _*_
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

type UnSealedTask struct {
	SectorID
	SrcIp         string
	OriSrc        string
	DstIp         string
	UnSealedSrc   string
	UnSealedDst   string
	TotalSize     int64
	Status        string
	SealProofType string
}

var _ Operation = &UnSealedTask{}

func newUnSealedTask(unSealedSrc, oriSrc, srcIP, sId string) (*UnSealedTask, error) {
	var task = new(UnSealedTask)
	oriSrc = strings.TrimRight(oriSrc, "/")

	stat, _ := os.Stat(unSealedSrc)
	if stat.Size() >= (34359738368-16<<10) && stat.Size() <= (34359738368+16<<10) {
		task.SealProofType = ProofType32G
	} else if stat.Size() >= (68719476736-16<<10) && stat.Size() <= (68719476736+16<<10) {
		task.SealProofType = ProofType64G
	} else {
		log.Warnf("sealed file %s size not 32G or 64G,we can not deal it now", unSealedSrc)
		return nil, nil
	}

	task.SectorID.ID = sId
	task.SrcIp = srcIP
	task.OriSrc = oriSrc
	task.UnSealedSrc = unSealedSrc
	task.TotalSize = stat.Size()
	task.Status = StatusOnWaiting
	return task, nil
}

func (t *UnSealedTask) canDo() bool {
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

func (t *UnSealedTask) getBestDst() (string, string, error) {
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
		sort.Slice(dstC.Paths, func(i, j int) bool {
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
		return "", "", errors.New(move_common.NoDstSuitableForNow)
	}

	return dir, s, nil
}

func (t *UnSealedTask) fullInfo(dstOri, dstIp string) {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	t.UnSealedDst = strings.Replace(t.UnSealedSrc, t.OriSrc, strings.TrimRight(dstOri, "/"), 1)
	t.DstIp = dstIp
}

func (t *UnSealedTask) startCopy(cfg *Config, dstPath string) {
	log.Infof("start to copying %v", *t)
	// copying unsealed
	err := copying(t.UnSealedSrc, t.UnSealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	freeThreads(dstPath, t.DstIp, t.SrcIp, t.OriSrc)
	if err != nil {
		if err.Error() == move_common.StoppedBySyscall {
			log.Warn(err)
		} else {
			log.Error(err)
		}
		os.Remove(t.UnSealedDst)
		os.Remove(t.UnSealedDst + ".tmp")
		if os.Getenv("SKIP_FAILED") == "1" {
			t.setStatus(StatusDone)
		} else {
			t.setStatus(StatusOnWaiting)
		}
	} else {
		t.setStatus(StatusDone)
		log.Infof("task %v done", *t)
	}
}

func (t *UnSealedTask) tryToFindGroupDir() (string, string, error) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	log.Debugf("trying to find group dir for %s unsealed", t.getSectorID())
	// search sealed at first
	for _, cmp := range dstComputersMapSingleton.CMap {
		for _, p := range cmp.Paths {
			dstSealed := strings.TrimRight(p.Location, "/") + "/sealed/" + t.getSectorID()
			_, err := os.Stat(dstSealed)
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
					log.Debugf("%v fond same group dir on %s, but too much threads for now, will copy later", *t, p.Location)
					return "", "", errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	// search cache
	for _, cmp := range dstComputersMapSingleton.CMap {
		for _, p := range cmp.Paths {
			dstCache := strings.TrimRight(p.Location, "/") + "/cache/" + t.getSectorID()
			_, err := os.Stat(dstCache)
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
					log.Debugf("%v fond same group dir on %s, but too much threads for now, will copy later", *t, p.Location)
					return "", "", errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	return "", "", errors.New("no same group dir")
}

func (t *UnSealedTask) getInfo() interface{} {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return *t
}

func (t *UnSealedTask) getStatus() string {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return t.Status
}

func (t *UnSealedTask) setStatus(st string) {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	t.Status = st
}

func (t *UnSealedTask) getSrcIp() string {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return t.SrcIp
}

func (t *UnSealedTask) checkSourceSize() ([]string, error) {
	var paths = make([]string, 0)

	size, err := getStandSize(t.SealProofType, t.UnSealedSrc)
	if err != nil {
		return paths, err
	}
	err = compareSize(t.UnSealedSrc, size, 16<<10)
	if err != nil {
		return paths, err
	}

	paths = append(paths, t.UnSealedSrc)
	return paths, nil
}

func (t *UnSealedTask) checkIsExistedInDst(srcPaths []string, cfg *Config) bool {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	sinceTime := time.Now()
	for _, v := range dstComputersMapSingleton.CMap {
		for _, p := range v.Paths {
			tag := 1
			for _, singleUnSealedPath := range srcPaths {
				dst := strings.Replace(singleUnSealedPath, t.OriSrc, p.Location, 1)
				statSrc, _ := os.Stat(singleUnSealedPath)
				statDst, err := os.Stat(dst)
				// if existed,check hash
				if err == nil {
					if statDst.Size() == statSrc.Size() {
						srcHash, _ := recordCalLogIfNeed(mv_utils.CalFileHash, singleUnSealedPath, statSrc.Size(), cfg.Chunks)
						dstHash, _ := recordCalLogIfNeed(mv_utils.CalFileHash, dst, statDst.Size(), cfg.Chunks)
						if srcHash == dstHash && srcHash != "" && dstHash != "" {
							tag = 1
						} else {
							tag = 0
						}
					} else {
						tag = 0
					}
				} else {
					tag = 0
				}
			}
			if tag == 1 {
				log.Debugf("src unsealed file: %v already existed in dst %s,unSealedTask done,check cost %v",
					*t, p.Location, time.Now().Sub(sinceTime))
				log.Debugf("task %v is existed in dst", *t)
				return true
			}
		}
	}
	return false
}

func (t *UnSealedTask) getSrcPath() string {
	taskListSingleton.TLock.Lock()
	defer taskListSingleton.TLock.Unlock()
	return t.OriSrc
}
