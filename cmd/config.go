/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/14 下午11:02 _*_
**/

package main

import (
	"errors"
	"fmt"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"move_sectors/mv_utils"
	"sync"
)

type Config struct {
	SrcComputers     []Computer
	DstComputers     []Computer
	SingleThreadMBPS int
	Chunks           int64
}

type Computer struct {
	Ip             string
	Path           []string
	BandWidth      int
	LimitThread    int
	CurrentThreads int
	TotalSize      uint64
	AvailableSize  uint64
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
	if cfg.SrcComputers == nil {
		return false, fmt.Errorf("src computers is nil")
	}
	if cfg.DstComputers == nil {
		return false, fmt.Errorf("dst computers is nil")
	}
	if err := initializeComputerMapSingleton(cfg); err != nil {
		return false, err
	}
	if cfg.SingleThreadMBPS == 0 {
		return false, fmt.Errorf("SingleThreadMBPS should not be zero,if you want to exit or hold copy,please use stop cmd or hold cmd")
	}
	if cfg.Chunks < 3 {
		return false, fmt.Errorf("lowest chunks required 3 but %d", cfg.Chunks)
	}
	return true, nil
}

func hasEnoughSpaceToStore(src, dst string) (bool, error) {
	srcSize, err := mv_utils.GetSrcSize(src)
	if err != nil {
		return false, fmt.Errorf("src path: %s %v", src, err)
	}
	availableSize, err := mv_utils.GetAvailableSize(dst)
	if err != nil {
		return false, fmt.Errorf("dst path: %s %v", dst, err)
	}
	if availableSize <= srcSize {
		return false, fmt.Errorf("dst: %s has no enough space to store all files from %s", dst, src)
	}
	return true, nil
}

func initializeComputerMapSingleton(cfg *Config) error {
	for _, v := range cfg.SrcComputers {
		if v.Ip == "" || v.BandWidth == 0 || len(v.Path) == 0 {
			return errors.New("invalid computer ip, BandWidth or paths; please check the config")
		}
		if _, ok := computersMapSingleton.CMap[v.Ip]; !ok {
			v.LimitThread = calThreadLimit(v.BandWidth, cfg.SingleThreadMBPS)
			computersMapSingleton.CMap[v.Ip] = v
			checkDoubled := make(map[string]struct{})
			for _, path := range v.Path {
				if _, ok = checkDoubled[path]; ok {
					return fmt.Errorf("doubled path:%s in same ip:%s", path, v.Ip)
				}
				checkDoubled[path] = struct{}{}
			}
		} else {
			return errors.New("double computer ip,please check the config")
		}
	}
	return nil
}
