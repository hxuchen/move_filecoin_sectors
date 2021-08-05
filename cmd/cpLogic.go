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
	"io"
	"move_sectors/move_common"
	"move_sectors/mv_utils"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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

func initOps() ([]Operation, error) {
	var ops = make([]Operation, 0)
	for _, srcComputer := range srcComputersMapSingleton.CMap {
		for _, src := range srcComputer.Paths {
			if stop {
				return nil, errors.New("stopped by signal")
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
					return nil, err
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
					return nil, err
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
					return nil, err
				}
			}
		}
	}
	return ops, nil

}

func checkSourceSizeAndIsExistedInDst(ops []Operation, cfg *Config) error {
	var threadChan = make(chan struct{}, runtime.NumCPU())
	wg := sync.WaitGroup{}
	if lenOps := len(ops); lenOps > 0 {
		lenSpecifiedMap := len(specifiedSectorsMap)
		for _, v := range ops {
			if stop {
				return nil
			}
			op := v
			// if manually specify sectors to copy,just check and copy specified sectors
			if lenSpecifiedMap > 0 {
				if _, ok := specifiedSectorsMap[op.getSectorID()]; !ok {
					continue
				}
			}
			// checkSourceSize
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
				wg.Add(1)
				go func() {
					defer func() {
						<-threadChan
						wg.Done()
					}()
					// check is already existed in dst
					if op.checkIsExistedInDst(srcPaths, cfg) {
						return
					}

					// add op
					taskListSingleton.TLock.Lock()
					taskListSingleton.Ops = append(taskListSingleton.Ops, op)
					taskListSingleton.TLock.Unlock()
				}()
			}
		}
	}

	// wait all thread done
	wg.Wait()
	close(threadChan)
	return nil
}

// init task list
func initializeTaskList(cfg *Config) error {
	log.Info("initializing tasks")

	// make ops slice
	ops, err := initOps()
	if err != nil {
		return err
	}

	// check source size && IsExistedInDst
	err = checkSourceSizeAndIsExistedInDst(ops, cfg)
	if err != nil {
		return err
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
	since := time.Now()
	//lenSpecifiedMap := len(specifiedSectorsMap)
	for {
		NotDoneNum := 0
		for _, v := range taskListSingleton.Ops {
			t := v
			if stop {
				log.Warn(move_common.StoppedBySyscall)
				waitingForAllTaskStop()
				return
			}

			switch t.getStatus() {
			case StatusOnWaiting:
				NotDoneNum++
				if t.canDo() {
					// get one best dst
					dst, dstIp, err := t.getBestDst()
					if err != nil {
						if err.Error() == move_common.FondGroupButTooMuchThread {
							continue
						} else if err.Error() == move_common.NoDstSuitableForNow {
							log.Debug(err.Error())
						} else {
							log.Warn(err)
						}
						continue
					}
					t.fullInfo(dst, dstIp)
					srcIp := t.getSrcIp()
					srcPath := t.getSrcPath()
					occupyThreads(dst, dstIp, srcIp, srcPath)
					t.setStatus(StatusOnWorking)
					go t.startCopy(cfg, dst)
				}
			case StatusOnWorking:
				NotDoneNum++
			case StatusDone:
			}
		}
		if NotDoneNum == 0 {
			break
		}
		if os.Getenv("SHOW_DETAIL") == "1" {
			if time.Now().Sub(since) > time.Minute*5 {
				since = time.Now()
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
					if ip != "" && v.CurrentThreads == 0 {
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
		}
		if fileType != move_common.Cache {
			time.Sleep(time.Second * 10)
		} else {
			time.Sleep(time.Second * 2)
		}
	}
	log.Infof("all task done for %s file", fileType)
}

func waitingForAllTaskStop() {
	log.Info("waiting all tasks stop to exit process")
	for {
		num := 0
		for _, t := range taskListSingleton.Ops {
			if t.getStatus() == StatusOnWorking {
				num++
			}
		}
		if num == 0 {
			log.Info("all tasks stopped")
			break
		}
		log.Infof("on working tasks remain %d", num)
		time.Sleep(time.Second)
	}
}

func makeSpecifiedSectorsMap(path string) error {
	absPath, err := mv_utils.GetAbsPath(path)
	if err != nil {
		return err
	}
	f, err := os.Open(absPath)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for {
		s, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		if _, ok := specifiedSectorsMap[string(s)]; ok {
			return fmt.Errorf("doubled sectorID in sectors list file %s", absPath)
		}
		specifiedSectorsMap[string(s)] = struct{}{}
	}
	return nil
}
