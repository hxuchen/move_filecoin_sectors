/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午8:33 _*_
**/

package build

const BaseVersion = "v2.0.1"

var CurrentCommit string

type CpVersion struct {
	BaseVersion string
	UserVersion string
}

func GetVersion() string {
	return BaseVersion + CurrentCommit
}
