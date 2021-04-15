/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"errors"
)

func startCopy(cfg *Config) {

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
