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
)

type Config struct {
	SrcComputers          []Computer
	DstComputers          []Computer
	SingleThreadMBPS      int
	SinglePathThreadLimit int
	Chunks                int64
}

type Computer struct {
	Ip             string
	Paths          []Path
	BandWidth      int
	LimitThread    int
	CurrentThreads int
}

type Path struct {
	Location       string
	CurrentThreads int64
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
	if cfg.SinglePathThreadLimit <= 0 {
		return false, errors.New("invalid single path thread limit")
	}
	if err := initializeComputerMapSingleton(cfg); err != nil {
		return false, err
	}
	if cfg.SingleThreadMBPS == 0 {
		return false, fmt.Errorf("SingleThreadMBPS should not be zero,if you want to exit or hold copying,please use stop cmd or hold cmd")
	}
	if cfg.Chunks < 3 {
		return false, fmt.Errorf("lowest chunks required 3 but %d", cfg.Chunks)
	}
	return true, nil
}

func initializeComputerMapSingleton(cfg *Config) error {
	for _, v := range cfg.SrcComputers {
		if v.Ip == "" || v.BandWidth == 0 || len(v.Paths) == 0 {
			return errors.New("invalid computer ip, BandWidth or paths; please check the config")
		}
		if _, ok := srcComputersMapSingleton.CMap[v.Ip]; !ok {
			v.LimitThread = calThreadLimit(v.BandWidth, cfg.SingleThreadMBPS)
			srcComputersMapSingleton.CMap[v.Ip] = v
			checkDoubled := make(map[string]struct{})
			for _, path := range v.Paths {
				if _, ok = checkDoubled[path.Location]; ok {
					return fmt.Errorf("doubled path:%s in same ip:%s", path, v.Ip)
				}
				checkDoubled[path.Location] = struct{}{}
			}
		} else {
			return errors.New("double computer ip,please check the config")
		}
	}

	for _, v := range cfg.DstComputers {
		if v.Ip == "" || v.BandWidth == 0 || len(v.Paths) == 0 {
			return errors.New("invalid computer ip, BandWidth or paths; please check the config")
		}
		if _, ok := dstComputersMapSingleton.CMap[v.Ip]; !ok {
			v.LimitThread = calThreadLimit(v.BandWidth, cfg.SingleThreadMBPS)
			dstComputersMapSingleton.CMap[v.Ip] = v
			checkDoubled := make(map[string]struct{})
			for _, path := range v.Paths {
				if _, ok = checkDoubled[path.Location]; ok {
					return fmt.Errorf("doubled path:%s in same ip:%s", path, v.Ip)
				}
				checkDoubled[path.Location] = struct{}{}
			}
		} else {
			return errors.New("double computer ip,please check the config")
		}
	}

	return nil
}
