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
	"runtime"
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
	log.Info("start to init tasks")
	var threadChan = make(chan struct{}, runtime.NumCPU())
	var lastOpDone = make(chan struct{}, 1)
	var ops = make([]Operation, 0)
	for _, srcComputer := range srcComputersMapSingleton.CMap {
		log.Debugf("%v", srcComputersMapSingleton.CMap)
		for _, src := range srcComputer.Paths {
			log.Debugf("dealing %s %s", srcComputer.Ip, src)
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
					if info.Mode().IsDir() && path != cacheSrcDir {
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

						ops = append(ops, cacheTask)
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
					sealedTask, err := newSealedTask(path, info.Name(), src.Location, srcComputer.Ip)
					if err != nil {
						return err
					}
					// do not cp file which isn't 32G or 64G,or which size error
					if sealedTask == nil {
						return nil
					}
					ops = append(ops, sealedTask)

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

					// do not cp file which isn't 32G or 64G,or which size error
					if unsealedTask == nil {
						return nil
					}

					ops = append(ops, unsealedTask)

					return err
				})
				if err != nil {
					return err
				}
			}
		}
	}

	if lenOps := len(ops); lenOps > 0 {
		for i, v := range ops {
			if stop {
				break
			}
			idx := i
			op := v
			// checkSourceSize
			log.Debugf("check source size of %v", op.getInfo())
			srcPaths, err := op.checkSourceSize()
			if err != nil {
				if skipSourceError {
					continue
				} else {
					return err
				}
			}

			select {
			case threadChan <- struct{}{}:
				go func() {
					defer func() {
						<-threadChan
						if idx == lenOps-1 {
							log.Debugf("last op check done,idx is %d,task is %v", idx, op.getInfo())
							lastOpDone <- struct{}{}
						}
					}()
					// check is already existed in dst
					log.Debugf("check file is already existed", op.getInfo())
					if op.checkIsExistedInDst(srcPaths, cfg) {
						return
					}

					// add op
					taskListSingleton.TLock.Lock()
					taskListSingleton.Ops = append(taskListSingleton.Ops, op)
					taskListSingleton.TLock.Unlock()

					log.Debugf("task %v init done", op.getInfo())
				}()
			}
		}
	}
	select {
	case <-lastOpDone:
		close(threadChan)
		close(lastOpDone)
	}
	log.Info("all tasks init done")
	return nil
}

func startWork(cfg *Config) {
	// init task list
	err := initializeTaskList(cfg)
	if err != nil {
		log.Error(err)
		return
	}
	for {
		allDone := true
		for _, v := range taskListSingleton.Ops {
			t := v
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
					log.Debugf("start to get best dst fot %v", t.getInfo())
					dst, dstIp, dstPathIdxInComp, err := t.getBestDst()
					log.Debugf("got best dst done for %v", t.getInfo())
					if err != nil {
						if err.Error() == move_common.FondGroupButTooMuchThread {
							continue
						} else if err.Error() == move_common.NoDstSuitableForNow {
							log.Debug(err.Error())
						} else {
							log.Warn(err)
						}
						t.releaseSrcComputer()
						t.releaseDstComputer()
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

		if allDone {
			break
		}
		if os.Getenv("SHOW_DETAIL") == "1" {
			fmt.Println("src computer thread info:")
			for ip, v := range srcComputersMapSingleton.CMap {
				fmt.Printf("%s: current thread:%d; limit thread:%d \n", ip, v.CurrentThreads, v.LimitThread)
			}

			fmt.Println("dst computer thread info:")
			for ip, v := range dstComputersMapSingleton.CMap {
				fmt.Printf("%s: current thread:%d; limit thread:%d \n", ip, v.CurrentThreads, v.LimitThread)
				for _, p := range v.Paths {
					fmt.Printf("path: %s,current thread:%d; limit thread:%d \n", p.Location, p.CurrentThreads, p.SinglePathThreadLimit)
				}
				if v.CurrentThreads == 0 {
					for _, ov := range taskListSingleton.Ops {
						info := ov.getInfo()
						if ov.getStatus() != StatusDone {
							switch fileType {
							case move_common.Sealed:
								task := info.(SealedTask)
								if task.DstIp == v.Ip {
									fmt.Println(task)
								}
							case move_common.Cache:
								task := info.(CacheTask)
								if task.DstIp == v.Ip {
									fmt.Println(task)
								}
							case move_common.UnSealed:
								task := info.(UnSealedTask)
								if task.DstIp == v.Ip {
									fmt.Println(task)
								}
							}
						}

					}
				}
			}

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
