/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午10:09 _*_
**/

package mv_utils

import (
	"github.com/mitchellh/go-homedir"
	"path/filepath"
)

func GetAbsPath(p string) (string, error) {
	newPath, err := homedir.Expand(p)
	if err != nil {
		return "", err
	}
	newPath, err = filepath.Abs(newPath)
	if err != nil {
		return "", err
	}
	return newPath, nil
}
