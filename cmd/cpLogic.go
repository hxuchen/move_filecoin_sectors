/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"errors"
	"fmt"
	"move_sectors/move_common"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
				if path.SinglePathThreadLimit <= 0 {
					return errors.New("invalid single path thread limit")
				}
				if _, ok = checkDoubled[path.Location]; ok {
					return fmt.Errorf("doubled path:%s in same ip:%s", path.Location, v.Ip)
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
				if path.SinglePathThreadLimit <= 0 {
					return errors.New("invalid single path thread limit")
				}
				if _, ok = checkDoubled[path.Location]; ok {
					return fmt.Errorf("doubled path:%s in same ip:%s", path.Location, v.Ip)
				}
				checkDoubled[path.Location] = struct{}{}
			}
		} else {
			return errors.New("double computer ip,please check the config")
		}
	}

	return nil
}

// init task list
func initializeTaskList(cfg *Config) error {
	for _, srcComputer := range srcComputersMapSingleton.CMap {
		for _, src := range srcComputer.Paths {
			if stop {
				return errors.New("stopped by signal")
			}
			switch fileType {
			case move_common.Cache:
				cacheSrcDir := strings.TrimRight(src.Location, "/") + "/cache"
				err := filepath.Walk(cacheSrcDir, func(path string, info os.FileInfo, err error) error {
					if stop {
						return errors.New(move_common.StoppedBySyscall)
					}
					if info == nil || err != nil {
						return err
					}
					if info.Mode().IsDir() {
						// get initialized cacheTask
						singleCacheSrcDir := cacheSrcDir + "/" + info.Name()
						cacheTask, err := newCacheTask(singleCacheSrcDir, info.Name(), src.Location, srcComputer.Ip)
						if err != nil {
							return err
						}
						// do not cp file which isn't 32G or 64G,or which size error
						if cacheTask == nil {
							return nil
						}

						if os.Getenv("SHOW_LOG_DETAIL") == "1" {
							log.Infof("task %v init done", cacheTask)
						}

						// add op
						taskListSingleton.Ops = append(taskListSingleton.Ops, cacheTask)
					}
					return err
				})
				if err != nil {
					return err
				}
			case move_common.Sealed:
				sealedSrcDir := strings.TrimRight(src.Location, "/") + "/sealed"
				err := filepath.Walk(sealedSrcDir, func(path string, info os.FileInfo, err error) error {
					if stop {
						return errors.New(move_common.StoppedBySyscall)
					}
					if info == nil || err != nil {
						return err
					}
					if !info.Mode().IsRegular() {
						return nil
					}
					cacheSealedTask, err := newSealedTask(path, info.Name(), src.Location, srcComputer.Ip)
					if err != nil {
						return err
					}
					// do not cp file which isn't 32G or 64G,or which size error
					if cacheSealedTask == nil {
						return nil
					}
					if cacheSealedTask.checkIsCopied(cfg) {
						cacheSealedTask.setStatus(StatusDone)
					}
					if os.Getenv("SHOW_LOG_DETAIL") == "1" {
						log.Infof("task %v init done", cacheSealedTask)
					}
					taskListSingleton.Ops = append(taskListSingleton.Ops, cacheSealedTask)
					return err
				})
				if err != nil {
					return err
				}
			case move_common.UnSealed:
				unsealedSrcDir := strings.TrimRight(src.Location, "/") + "/unsealed"
				err := filepath.Walk(unsealedSrcDir, func(path string, info os.FileInfo, err error) error {
					if stop {
						return errors.New(move_common.StoppedBySyscall)
					}
					if info == nil || err != nil {
						return err
					}
					if !info.Mode().IsRegular() {
						return nil
					}
					unsealedTask, err := newUnSealedTask(path, src.Location, srcComputer.Ip, info.Name())
					if err != nil {
						return err
					}
					if unsealedTask.checkIsCopied(cfg) {
						unsealedTask.setStatus(StatusDone)
					}
					if os.Getenv("SHOW_LOG_DETAIL") == "1" {
						log.Infof("task %v init done", unsealedTask)
					}
					taskListSingleton.Ops = append(taskListSingleton.Ops, unsealedTask)
					return err
				})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func startWork(cfg *Config) {
	// init task list
	log.Info("start to init tasks")
	err := initializeTaskList(cfg)
	log.Info("tasks init done")
	if err != nil {
		log.Error(err)
		return
	}
	for {
		allDone := true
		for _, t := range taskListSingleton.Ops {
			if stop {
				log.Warn(move_common.StoppedBySyscall)
				waitingForAllTaskStop()
				return
			}
			switch t.getStatus() {
			case StatusOnWaiting:
				allDone = false
				if t.canDo() {
					// get one best dst
					dst, dstIp, dstPathIdxInComp, err := t.getBestDst()
					if err != nil {
						if err.Error() != move_common.NoDstSuitableForNow {
							log.Warn(err)
						} else {
							if os.Getenv("SHOW_LOG_DETAIL") == "1" {
								log.Warn(err)
							}
						}
						t.releaseSrcComputer()
						t.releaseDstComputer()
						continue
					}
					taskListSingleton.TLock.Lock()
					t.setStatus(StatusOnWorking)
					taskListSingleton.TLock.Unlock()
					t.fullInfo(dst, dstIp)
					go t.startCopy(cfg, dstPathIdxInComp)
				}
			case StatusOnWorking:
				allDone = false
			case StatusDone:
			}
		}

		if allDone {
			break
		}
		time.Sleep(time.Second * 5)
	}
	log.Info("all task done")
}

func waitingForAllTaskStop() {
	log.Info("waiting all tasks stop to exit process")
	allStop := true
	for {
		for _, t := range taskListSingleton.Ops {
			if t.getStatus() == StatusOnWorking {
				allStop = false
			}
		}
		if allStop {
			log.Info("all tasks stopped")
			break
		}
		time.Sleep(time.Second)
	}
}
