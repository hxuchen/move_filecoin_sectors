/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/15 下午9:58 _*_
**/

package main

import "time"

func calThreadLimit(bindWidth, singleThreadMBPS int) int {
	return bindWidth << 10 / singleThreadMBPS
}

func canGo(srcComputer, dstComputer *Computer) chan struct{} {
	var ok = make(chan struct{}, 1)
	for {
		if srcComputer.CurrentThreads < srcComputer.LimitThread && dstComputer.CurrentThreads < dstComputer.LimitThread {
			ok <- struct{}{}
			break
		}
		time.Sleep(time.Second)
	}
	return ok
}

//func calCopyCycleDelay(bindWidth, singleThreadMBPS int) int {
//	return bindWidth << 10 / singleThreadMBPS
//}
