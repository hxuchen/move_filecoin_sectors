/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/27 下午4:49 _*_
**/

package main

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/big"
	"move_sectors/move_common"
	"move_sectors/mv_utils"
	"os"
	"sort"
	"strings"
	"syscall"
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
	task.SectorID = sectorID
	task.SrcIp = srcIP
	task.OriSrc = oriSrc
	task.UnSealedSrc = unSealedSrc
	stat, _ := os.Stat(unSealedSrc)
	if stat.Size() >= (34359738368-16<<10) && stat.Size() <= (34359738368+16<<10) {
		task.SealProofType = ProofType32G
	} else if stat.Size() >= (68719476736-16<<10) && stat.Size() <= (68719476736+16<<10) {
		task.SealProofType = ProofType64G
	} else {
		log.Errorf("sealed file %s size not 32G or 64G,we can not deal it now", unSealedSrc)
		return nil, nil
	}
	task.TotalSize = stat.Size()
	task.Status = StatusOnWaiting
	return task, nil
}

func (t *UnSealedTask) printInfo() {
	fmt.Println(*t)
}

func (t *UnSealedTask) getBestDst() (string, string, int, error) {
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
	srcComputer.CurrentThreads--
	srcComputersMapSingleton.CMap[t.SrcIp] = srcComputer
}

func (t *UnSealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.DstIp]
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComputer
}

func (t *UnSealedTask) getStatus() string {
	return t.Status
}

func (t *UnSealedTask) setStatus(st string) {
	t.Status = st
}

func (t *UnSealedTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	// copying unsealed
	err := copying(t.UnSealedSrc, t.UnSealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		if err.Error() == move_common.StoppedBySyscall {
			log.Warn(err)
		} else {
			log.Error(err)
		}
		t.releaseSrcComputer()
		t.releaseDstComputer()
		os.Remove(t.UnSealedDst)
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
}

func (t *UnSealedTask) fullInfo(dstOri, dstIp string) {
	t.UnSealedDst = strings.Replace(t.UnSealedSrc, t.OriSrc, dstOri, 1)
	t.DstIp = dstIp
}

func (t *UnSealedTask) occupyDstPathThread(idx int, c *Computer) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	c.Paths[idx].CurrentThreads++
	dstComputersMapSingleton.CMap[c.Ip] = *c
}

func (t *UnSealedTask) freeDstPathThread(idx int) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComp := dstComputersMapSingleton.CMap[t.DstIp]
	dstComp.Paths[idx].CurrentThreads--
	dstComputersMapSingleton.CMap[t.DstIp] = dstComp
}

func (t *UnSealedTask) makeDstPathSliceForCheckIsCopied(oriDst string) ([]string, error) {
	return nil, nil
}

func (t *UnSealedTask) checkIsCopied(cfg *Config) bool {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	for _, v := range dstComputersMapSingleton.CMap {
		for _, p := range v.Paths {
			fileDstPath := strings.Replace(t.UnSealedSrc, t.OriSrc, p.Location, 1)
			statSrc, _ := os.Stat(t.UnSealedSrc)
			statDst, err := os.Stat(fileDstPath)
			// if existed,check hash
			if err == nil {
				if statDst.Size() == statSrc.Size() {
					srcHash, _ := mv_utils.CalFileHash(t.UnSealedSrc, statSrc.Size(), cfg.Chunks)
					dstHash, _ := mv_utils.CalFileHash(fileDstPath, statDst.Size(), cfg.Chunks)
					if srcHash == dstHash && srcHash != "" && dstHash != "" {
						return true
					}
				}
			}
		}
	}
	return false
}
