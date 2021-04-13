/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:41 _*_
**/

package mv_common

import "sync"

type SpeedMod struct {
	Lock *sync.RWMutex
	Mod  int
}

type SrcFiles struct {
	Path  string
	Usage uint64
}
