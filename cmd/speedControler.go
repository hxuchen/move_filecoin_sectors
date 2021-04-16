/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/15 下午9:58 _*_
**/

package main

func calThreadLimit(bindWidth, singleThreadMBPS int) int {
	return bindWidth << 10 / singleThreadMBPS
}

func canGo(srcComputer, dstComputer *Computer) bool {
	return srcComputer.CurrentThreads < srcComputer.LimitThread && dstComputer.CurrentThreads < dstComputer.LimitThread
}
