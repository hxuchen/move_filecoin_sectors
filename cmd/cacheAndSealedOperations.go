/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/26 上午11:50 _*_
**/

package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type cacheSealedTask struct {
	srcIp       string
	dstIp       string
	cacheSrcDir string
	sealedSrc   string
	cacheDstDir string
	sealedDst   string
}

func newCacheSealedTask(sealedSrc, oriSrc, oriDst, srcIP, dstIP string, skipCheckSrc bool) (*cacheSealedTask, error) {
	var task = new(cacheSealedTask)
	oriSrc = strings.TrimRight(oriSrc, "/")
	oriDst = strings.TrimRight(oriDst, "/")
	cacheSrcDir := oriSrc + "/" + "cache"

	if !skipCheckSrc {
		cacheSrcDir = checkAndFindCacheSrc(cacheSrcDir, oriSrc)
		if cacheSrcDir == "" {
			return task, fmt.Errorf("%s: %s", SourceFileNotExisted, cacheSrcDir)
		}
	}
	// splice dst paths
	sealedDst := strings.Replace(sealedSrc, oriSrc, oriDst, 1)
	cacheDstDir := strings.Replace(cacheSrcDir, oriSrc, oriDst, 1)

	task.srcIp = srcIP
	task.dstIp = dstIP
	task.cacheSrcDir = cacheSrcDir
	task.sealedSrc = sealedSrc
	task.sealedDst = sealedDst
	task.cacheDstDir = cacheDstDir

	return task, nil
}

func (t *cacheSealedTask) canDo() {
	computersMapSingleton.CLock.Lock()
	defer computersMapSingleton.CLock.Unlock()
	srcComputer := computersMapSingleton.CMap[t.srcIp]
	dstComputer := computersMapSingleton.CMap[t.dstIp]
	for {
		if srcComputer.CurrentThreads < srcComputer.LimitThread && dstComputer.CurrentThreads < dstComputer.LimitThread {
			threadControlChan <- struct{}{}
			break
		}
		time.Sleep(time.Second)
	}
}

func (t *cacheSealedTask) startCopy() {
	// copy cache

	// copy sealed

	// copy unsealed

}

func checkAndFindCacheSrc(cacheSrcDir, oriSrc string) string {
	// verify all src files existed, if not existed, find all computers
	var needFindCacheSrcDir bool
	if _, errCacheSrcDir := os.Stat(cacheSrcDir); errCacheSrcDir != nil && os.IsNotExist(errCacheSrcDir) {
		cacheSrcDir = ""
		needFindCacheSrcDir = true
	}
	if needFindCacheSrcDir {
		var errFind error
	FindCacheSrcLoopCache:
		for _, comp := range computersMapSingleton.CMap {
			for _, singlePath := range comp.Path {
				cacheSrcDirTmp := strings.Replace(cacheSrcDir, oriSrc, strings.TrimRight(singlePath, "/"), 1)
				if _, errFind = os.Stat(cacheSrcDirTmp); errFind == nil {
					cacheSrcDir = cacheSrcDirTmp
					break FindCacheSrcLoopCache
				}
			}
		}
	}
	return cacheSrcDir
}
