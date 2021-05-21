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
	"sort"
	"strings"
	"syscall"
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

func (t *SealedTask) printInfo() {
	fmt.Println(*t)
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
	// copying cache
	//err := copyDir(t.CacheSrcDir, t.CacheDstDir, cfg)
	//if err != nil {
	//	if err.Error() == move_common.StoppedBySyscall {
	//		log.Warn(err)
	//	} else {
	//		log.Error(err)
	//	}
	//	t.releaseSrcComputer()
	//	t.releaseDstComputer()
	//	t.freeDstPathThread(dstPathIdxInComp)
	//	os.RemoveAll(t.CacheDstDir)
	//	taskListSingleton.TLock.Lock()
	//	t.setStatus(StatusOnWaiting)
	//	taskListSingleton.TLock.Unlock()
	//	return
	//}
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

func (t *SealedTask) makeDstPathSliceForCheckIsCopied(oriDst string) ([]string, error) {
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
		//cacheDir = t.CacheSrcDir
		sealedPath = t.SealedSrc
	} else {
		//cacheDir = strings.Replace(t.CacheSrcDir, t.OriSrc, oriDst, 1)
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

func (t *SealedTask) checkIsCopied(cfg *Config) bool {
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
