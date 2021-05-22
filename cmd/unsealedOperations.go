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
	SectorID      string
	SrcIp         string
	OriSrc        string
	DstIp         string
	UnSealedSrc   string
	UnSealedDst   string
	TotalSize     int64
	Status        string
	SealProofType string
}

func newUnSealedTask(unSealedSrc, oriSrc, srcIP, sectorID string) (*UnSealedTask, error) {
	var task = new(UnSealedTask)
	stat, _ := os.Stat(unSealedSrc)
	if stat.Size() >= (34359738368-16<<10) && stat.Size() <= (34359738368+16<<10) {
		task.SealProofType = ProofType32G
	} else if stat.Size() >= (68719476736-16<<10) && stat.Size() <= (68719476736+16<<10) {
		task.SealProofType = ProofType64G
	} else {
		log.Warnf("sealed file %s size not 32G or 64G,we can not deal it now", unSealedSrc)
		return nil, nil
	}

	task.SectorID = sectorID
	task.SrcIp = srcIP
	task.OriSrc = oriSrc
	task.UnSealedSrc = unSealedSrc
	task.TotalSize = stat.Size()
	task.Status = StatusOnWaiting
	return task, nil
}

func (t *UnSealedTask) getInfo() interface{} {
	return *t
}

func (t *UnSealedTask) getBestDst() (string, string, int, error) {
	log.Debugf("finding best dst, %s", t.SectorID)

	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()

	dir, s, i, err := t.tryToFindGroupDir()
	if err != nil {
		if err.Error() == move_common.FondGroupButTooMuchThread {
			return "", "", 0, err
		}

		dstC, err := getOneFreeDstComputer()
		if err != nil {
			return "", "", 0, err
		}

		log.Debugf("sorting dst paths")
		sort.Slice(dstC.Paths, func(i, j int) bool {
			iw := big.NewInt(dstC.Paths[i].CurrentThreads)
			jw := big.NewInt(dstC.Paths[j].CurrentThreads)
			return iw.GreaterThanEqual(jw)
		})

		log.Debugf("selecting dst paths for %s", t.SectorID)
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

func (t *UnSealedTask) canDo() bool {
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

func (t *UnSealedTask) releaseSrcComputer() {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.SrcIp]
	if srcComputer.CurrentThreads > 0 {
		srcComputer.CurrentThreads--
	}
	srcComputersMapSingleton.CMap[t.SrcIp] = srcComputer
}

func (t *UnSealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.DstIp]
	if dstComputer.CurrentThreads > 0 {
		dstComputer.CurrentThreads--
	}
	dstComputersMapSingleton.CMap[t.DstIp] = dstComputer
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

func (t *UnSealedTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	log.Infof("start to copying %v", *t)
	// copying unsealed
	err := copying(t.UnSealedSrc, t.UnSealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		if err.Error() == move_common.StoppedBySyscall {
			log.Warn(err)
		} else {
			log.Error(err)
		}
		os.Remove(t.UnSealedDst)
		t.setStatus(StatusOnWaiting)
	} else {
		t.setStatus(StatusDone)
		log.Infof("task %v done", *t)
	}
	t.releaseSrcComputer()
	t.releaseDstComputer()
	t.freeDstPathThread(dstPathIdxInComp)

}

func (t *UnSealedTask) fullInfo(dstOri, dstIp string) {
	t.UnSealedDst = strings.Replace(t.UnSealedSrc, t.OriSrc, dstOri, 1)
	t.DstIp = dstIp
}

func (t *UnSealedTask) occupyDstPathThread(idx int, c *Computer) {
	c.Paths[idx].CurrentThreads++
	dstComputersMapSingleton.CMap[c.Ip] = *c
}

func (t *UnSealedTask) freeDstPathThread(idx int) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComp := dstComputersMapSingleton.CMap[t.DstIp]
	if dstComp.Paths[idx].CurrentThreads > 0 {
		dstComp.Paths[idx].CurrentThreads--
	}
	dstComputersMapSingleton.CMap[t.DstIp] = dstComp
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
				log.Debugf("src unsealed file: %v already existed in dst %s,unSealedTask done,check cost %v",
					*t, p.Location, time.Now().Sub(sinceTime))
				log.Debugf("task %v is existed in dst", *t)
				return true
			}
		}
	}
	return false
}

func (t *UnSealedTask) tryToFindGroupDir() (string, string, int, error) {
	log.Debugf("trying to find group dir for %s unsealed", t.SectorID)
	// search sealed at first
	for _, cmp := range dstComputersMapSingleton.CMap {
		for idx, p := range cmp.Paths {
			dstSealed := strings.TrimRight(p.Location, "/") + "/sealed/" + t.SectorID
			_, err := os.Stat(dstSealed)
			if err == nil {
				if cmp.CurrentThreads < cmp.LimitThread && p.CurrentThreads < p.SinglePathThreadLimit {
					t.occupyDstPathThread(idx, &cmp)
					cmp.CurrentThreads++
					dstComputersMapSingleton.CMap[cmp.Ip] = cmp
					return p.Location, cmp.Ip, idx, nil
				} else {
					log.Debugf("%v fond same group dir on %s, but too much threads for now, will copy later", *t, p.Location)
					return "", "", 0, errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	// search cache
	for _, cmp := range dstComputersMapSingleton.CMap {
		for idx, p := range cmp.Paths {
			dstCache := strings.TrimRight(p.Location, "/") + "/cache/" + t.SectorID
			_, err := os.Stat(dstCache)
			if err == nil {
				if cmp.CurrentThreads < cmp.LimitThread && p.CurrentThreads < p.SinglePathThreadLimit {
					return p.Location, cmp.Ip, idx, nil
				} else {
					log.Debugf("%v fond same group dir on %s, but too much threads for now, will copy later", *t, p.Location)
					return "", "", 0, errors.New(move_common.FondGroupButTooMuchThread)
				}
			}
		}
	}

	return "", "", 0, errors.New("no same group dir")
}
