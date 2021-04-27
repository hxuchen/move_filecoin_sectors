/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/27 下午4:49 _*_
**/

package main

import (
	"strings"
	"time"
)

type unSealedTask struct {
	srcIp       string
	dstIp       string
	unSealedSrc string
	unSealedDst string
}

func newUnSealedTask(unSealedSrc, oriSrc, oriDst, srcIP, dstIP string, skipCheckSrc bool) (*unSealedTask, error) {
	var task = new(unSealedTask)
	oriSrc = strings.TrimRight(oriSrc, "/")
	oriDst = strings.TrimRight(oriDst, "/")

	// splice dst paths
	unSealedDst := strings.Replace(unSealedSrc, oriSrc, oriDst, 1)

	task.srcIp = srcIP
	task.dstIp = dstIP
	task.unSealedSrc = unSealedSrc
	task.unSealedDst = unSealedDst

	return task, nil
}

func (t *unSealedTask) canDo() {
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

func (t *unSealedTask) startCopy() {
	// copy unsealed

}
