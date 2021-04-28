/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/27 下午4:49 _*_
**/

package main

import (
	"errors"
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
	task.unSealedSrc = unSealedSrc
	stat, _ := os.Stat(unSealedSrc)
	task.totalSize = stat.Size()
	task.status = StatusOnWaiting
	return task, nil
}

func (t *UnSealedTask) getBestDst() (string, string, error) {
	dstC, err := getOneFreeDstComputer()
	if err != nil {
		return "", "", err
	}

	sort.Slice(dstC.Paths, func(i, j int) bool {
		iw := big.NewInt(dstC.Paths[i].CurrentThreads)
		jw := big.NewInt(dstC.Paths[j].CurrentThreads)
		return iw.GreaterThanEqual(jw)
	})

	for _, p := range dstC.Paths {
		var stat = new(syscall.Statfs_t)
		_ = syscall.Statfs(p.Location, stat)
		if stat.Bavail*uint64(stat.Bsize) > uint64(t.totalSize) {
			dstC.occupyDstThread()
			return p.Location, dstC.Ip, nil
		}
	}
	return "", "", errors.New("no dst has enough size to store for now,will try again later")
}

func (t *UnSealedTask) canDo() bool {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.srcIp]
	if srcComputer.CurrentThreads < srcComputer.LimitThread {
		srcComputer.occupySrcThread()
		return true
	}
	return false
}

func (t *UnSealedTask) releaseSrcComputer() {
	srcComputersMapSingleton.CLock.Lock()
	defer srcComputersMapSingleton.CLock.Unlock()
	srcComputer := srcComputersMapSingleton.CMap[t.srcIp]
	srcComputer.freeSrcThread()
}

func (t *UnSealedTask) releaseDstComputer() {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	dstComputer := dstComputersMapSingleton.CMap[t.dstIp]
	dstComputer.freeDstThread()
	dstComputersMapSingleton.CMap[t.dstIp] = dstComputer
}

func (t *UnSealedTask) getStatus() string {
	return t.status
}

func (t *UnSealedTask) setStatus(st string) {
	t.status = st
}

func (t *UnSealedTask) startCopy(cfg *Config) {
	// copy unsealed
	err := copy(t.unSealedSrc, t.unSealedDst, cfg.SingleThreadMBPS, cfg.Chunks)
	if err != nil {
		log.Error(err)
		t.releaseSrcComputer()
		t.releaseDstComputer()
		return
	}
	t.setStatus(StatusDone)
	t.releaseSrcComputer()
	t.releaseDstComputer()
}

func (t *UnSealedTask) fullInfo(dstOri, dstIp string) {
	t.unSealedDst = strings.Replace(t.unSealedSrc, t.oriSrc, dstOri, 1)
	t.dstIp = dstIp
}
