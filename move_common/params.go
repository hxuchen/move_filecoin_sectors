/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/5/21 上午9:40 _*_
**/

package move_common

type FileType string

const (
	Sealed   FileType = "Sealed"
	UnSealed FileType = "UnSealed"
	Cache    FileType = "Cache"
)
