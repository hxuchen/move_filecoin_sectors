/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"errors"
	"move_sectors/move_common"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// init task list
func initializeTaskList(cfg *Config) error {
	for _, srcComputer := range srcComputersMapSingleton.CMap {
		for _, src := range srcComputer.Paths {
			if stop {
				return errors.New("stopped by signal")
			}
			if !doUnSealed {
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
					cacheSealedTask, err := newCacheSealedTask(path, info.Name(), src.Location, srcComputer.Ip)

					// TODO: deal cache file not exists error
					if err != nil && strings.Contains(err.Error(), move_common.SourceFileNotExisted) {
						return nil
					}

					if err != nil {
						return err
					}
					// do not cp zero size file
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
			} else {
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
