/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"bufio"
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"move_sectors/mv_common"
	"move_sectors/mv_utils"
	"os"
	"path"
	"time"
)

func startCopy() {

}

// initialize src paths
func initializeSrcPathList(srcPathFile string) (uint64, error) {
	fi, err := os.Open(srcPathFile)
	if err != nil {
		return 0, err
	}
	defer fi.Close()
	var totalUsage uint64
	br := bufio.NewReader(fi)
	for {
		singlePath, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		if usage, err := mv_utils.GetUsedSize(string(singlePath)); err != nil {
			return 0, err
		} else {
			srcPathList = append(srcPathList,
				mv_common.SrcFiles{
					Path:     string(singlePath),
					Usage:    usage,
					SpeedMod: mv_common.FastMod,
				})
			totalUsage += usage
		}
	}
	return totalUsage, nil
}

func initializeComputerMapSingleton(cfg *Config) error {
	for _, v := range cfg.Computers {
		if v.Ip == "" || v.BindWidth == 0 {
			return errors.New("invalid computer ip or BindWidth,please check the config")
		}
		if computer, ok := computersMapSingleton[v.Ip]; !ok {
			computersMapSingleton[v.Ip] = computer
		} else {
			return errors.New("double computer ip,please check the config")
		}
	}
	return nil
}
