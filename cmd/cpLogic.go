/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// init task list
func initializeTaskList() error {
	for _, srcComputer := range srcComputersMapSingleton.CMap {
		for _, src := range srcComputer.Paths {
			if stop {
				return errors.New("stopped by signal")
			}
			if !doUnSealed {
				sealedSrcDir := strings.TrimRight(src.Location, "/") + "/sealed"
				err := filepath.Walk(sealedSrcDir, func(path string, info os.FileInfo, err error) error {
					if stop {
						return errors.New("stopped by signal")
					}
					if info == nil || err != nil {
						return err
					}
					if !info.Mode().IsRegular() {
						return nil
					}
					cacheSealedTask, err := newCacheSealedTask(path, src.Location, srcComputer.Ip)
					if err != nil {
						return err
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
				filepath.Walk(unsealedSrcDir, func(path string, info os.FileInfo, err error) error {
					if stop {
						return errors.New("stopped by signal")
					}
					if info == nil || err != nil {
						return err
					}
					if !info.Mode().IsRegular() {
						return nil
					}
					unsealedTask, err := newUnSealedTask(path, src.Location, srcComputer.Ip)
					if err != nil {
						return err
					}
					if os.Getenv("INIT_TASK_DETAIL") == "1" {
						log.Infof("task %v init done", unsealedTask)
					}
					taskListSingleton.Ops = append(taskListSingleton.Ops, unsealedTask)
					return err
				})

			}
		}
	}
	log.Info("tasks init done")
	return nil
}

func startWork(cfg *Config) {
	// init task list
	err := initializeTaskList()
	if err != nil {
		log.Error(err)
		return
	}
	for {
		if stop {
			log.Warn("task stopped by signal")
			return
		}
		allDone := true
		taskListSingleton.TLock.Lock()
		for _, t := range taskListSingleton.Ops {
			if stop {
				log.Warn("task stopped by signal")
				return
			}
			switch t.getStatus() {
			case StatusOnWaiting:
				allDone = false
				if t.canDo() {
					// get one best dst
					dst, dstIp, dstPathIdxInComp, err := t.getBestDst()
					if err != nil {
						t.releaseSrcComputer()
						log.Warn(err)
						continue
					}
					t.setStatus(StatusOnWorking)
					t.fullInfo(dst, dstIp)
					go t.startCopy(cfg, dstPathIdxInComp)
				}
			case StatusOnWorking:
				allDone = false
			case StatusDone:
			}
		}
		taskListSingleton.TLock.Unlock()
		if allDone {
			break
		}
		time.Sleep(time.Second * 5)
	}
	log.Info("all task done")
}
