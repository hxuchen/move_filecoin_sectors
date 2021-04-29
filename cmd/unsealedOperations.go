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
	"os"
	"sort"
	"strings"
	"syscall"
)

type UnSealedTask struct {
	srcIp       string
	oriSrc      string
	dstIp       string
	unSealedSrc string
	unSealedDst string
	totalSize   int64
	status      string
}

func newUnSealedTask(unSealedSrc, oriSrc, srcIP string) (*UnSealedTask, error) {
	var task = new(UnSealedTask)
	task.srcIp = srcIP
	task.oriSrc = oriSrc
	task.unSealedSrc = unSealedSrc
	stat, _ := os.Stat(unSealedSrc)
	task.totalSize = stat.Size()
	task.status = StatusOnWaiting
	return task, nil
}

func (t *UnSealedTask) printInfo() {
	fmt.Println(*t)
}

func (t *UnSealedTask) getBestDst(singlePathThreadLimit int) (string, string, int, error) {
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
	return "", "", 0, errors.New("no dst suitable for now,will try again later")
}

func (t *UnSealedTask) canDo() bool {
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

func (t *UnSealedTask) releaseSrcComputer() {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.srcIp]
	srcComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.srcIp] = srcComputer
}

func (t *UnSealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.dstIp]
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[t.dstIp] = dstComputer
}

func (t *UnSealedTask) getStatus() string {
	return t.status
}

func (t *UnSealedTask) setStatus(st string) {
	t.status = st
}

func (t *UnSealedTask) startCopy(cfg *Config, dstPathIdxInComp int) {
	// copy unsealed
	err := copy(t.unSealedSrc, t.unSealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		taskListSingleton.TLock.Lock()
		t.setStatus(StatusOnWaiting)
		taskListSingleton.TLock.Unlock()
		log.Error(err)
		t.releaseSrcComputer()
		t.releaseDstComputer()
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
	t.unSealedDst = strings.Replace(t.unSealedSrc, t.oriSrc, dstOri, 1)
	t.dstIp = dstIp
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
	dstComp := dstComputersMapSingleton.CMap[t.dstIp]
	dstComp.Paths[idx].CurrentThreads--
	dstComputersMapSingleton.CMap[t.dstIp] = dstComp
}
