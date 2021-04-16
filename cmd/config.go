/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/14 下午11:02 _*_
**/

package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"move_sectors/mv_utils"
	"sync"
)

type Config struct {
	Computers        []Computer
	CpTasks          []CpTask
	SingleThreadMBPS int
}

type Computer struct {
	Ip             string
	BandWidth      int
	LimitThread    int
	CurrentThreads int
}

type ComputersMap struct {
	CMap  map[string]Computer
	CLock *sync.Mutex
}

type CpTask struct {
	SrcIp string
	Src   string
	DstIp string
	Dst   string
}

func getConfig(cctx *cli.Context) (*Config, error) {
	configFilePath := cctx.String("path")
	configFilePath, err := mv_utils.GetAbsPath(configFilePath)
	if err != nil {
		return nil, err
	}
	config, err := LoadConfigFromFile(configFilePath)
	if err != nil {
		return nil, err
	}
	if qualifiedConfig, err := isQualifiedConfig(config); !qualifiedConfig {
		return nil, fmt.Errorf("config file: %v error:%v", configFilePath, err)
	}
	return config, nil
}

func LoadConfigFromFile(filePath string) (*Config, error) {
	raw, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	config := Config{}
	err = yaml.Unmarshal(raw, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func isQualifiedConfig(cfg *Config) (bool, error) {
	if cfg.Computers == nil {
		return false, fmt.Errorf("computers is nil")
	}
	if err := initializeComputerMapSingleton(cfg); err != nil {
		return false, err
	}
	if len(cfg.CpTasks) == 0 {
		return false, fmt.Errorf("has no task todo")
	}
	tMap := make(map[string]struct{})
	doubledTlist := make([]CpTask, 0)
	for _, t := range cfg.CpTasks {
		if t.Dst == "" || t.DstIp == "" || t.Src == "" || t.SrcIp == "" || t.Dst == t.Src {
			return false, fmt.Errorf("invalid task config:%v", t)
		}

		if _, ok := tMap[t.SrcIp]; ok {
			doubledTlist = append(doubledTlist, t)
		} else {
			tMap[t.SrcIp] = struct{}{}
		}
		if len(doubledTlist) > 0 {
			return false, fmt.Errorf("has doubed src paths,%v", doubledTlist)
		}

		if t.Dst == t.Src {
			return false, fmt.Errorf("dst: %s and src: %s should not be same", t.Dst, t.Src)
		}

		if has, err := hasEnoughSpaceToStore(t.Src, t.Dst); !has {
			return false, err
		}
	}
	if cfg.SingleThreadMBPS == 0 {
		return false, fmt.Errorf("SingleThreadMBPS should not be zero,if you want to exit or hold copy,please use stop cmd or hold cmd")
	}
	return true, nil
}

func hasEnoughSpaceToStore(src, dst string) (bool, error) {
	srcSize, err := mv_utils.GetUsedSize(src)
	if err != nil {
		return false, fmt.Errorf("path: %s %v", src, err)
	}
	availableSize, err := mv_utils.GetAvailableSize(dst)
	if err != nil {
		return false, fmt.Errorf("path: %s %v", dst, err)
	}
	if availableSize <= srcSize {
		return false, fmt.Errorf("dst: %s has no enough space to store all files from %s", dst, src)
	}
	return true, nil
}
