/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"errors"
	"fmt"
	"io"
	"move_sectors/mv_utils"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type WorkingTasks struct {
	TaskList []*CpTask
	WLock    *sync.Mutex
}

var workingTasks = new(WorkingTasks)

func start(cfg *Config) {
	stopSignal := make(chan os.Signal, 2)
	signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
	for _, task := range cfg.CpTasks {
		srcComputer, dstComputer := computersMapSingleton.CMap[task.SrcIp], computersMapSingleton.CMap[task.DstIp]
		select {
		case <-canGo(&srcComputer, &dstComputer):
			// thread ++
			computersMapSingleton.CLock.Lock()
			srcComputer.CurrentThreads++
			dstComputer.CurrentThreads++
			computersMapSingleton.CLock.Unlock()
			workingTasks.TaskList = append(workingTasks.TaskList, &task)
			copyCycleDelay := calCopyCycleDelay(srcComputer.BandWidth, cfg.SingleThreadMBPS)
			go copyGo(task, copyCycleDelay)
			time.Sleep(time.Second * 1)
		case <-stopSignal:
			waitingForTasksThreadsStop()
			break
		}
	}
}

func initializeComputerMapSingleton(cfg *Config) error {
	for _, v := range cfg.Computers {
		if v.Ip == "" || v.BandWidth == 0 {
			return errors.New("invalid computer ip or BandWidth,please check the config")
		}
		if computer, ok := computersMapSingleton.CMap[v.Ip]; !ok {
			computer.LimitThread = calThreadLimit(computer.BandWidth, cfg.SingleThreadMBPS)
			computersMapSingleton.CMap[v.Ip] = computer

		} else {
			return errors.New("double computer ip,please check the config")
		}
	}
	return nil
}

func copyGo(task CpTask, copyCycleDelay int) {
	stat, err := os.Stat(task.Src)
	if err != nil {
		log.Error(err)
		return
	}
	if stat.IsDir() {
		_ = filepath.Walk(task.Src, func(path string, srcF os.FileInfo, err error) error {
			if err != nil || srcF == nil {
				log.Error(err)
				return err
			}

			dst := getFinalDst(task.Src, path, task.Dst)
			dstF, err := os.Stat(dst)
			if err == nil && !dstF.IsDir() {
				srcSha256, _ := mv_utils.CalFileSha256(path, srcF.Size())
				dstSha256, _ := mv_utils.CalFileSha256(dst, dstF.Size())
				if srcSha256 == dstSha256 {
					return nil
				}
			}
			err = mv_utils.MakeDirIfNotExists(dst)
			if err != nil {
				log.Error(err)
				return err
			}
			err = copy(path, dst, copyCycleDelay)
			if err != nil {
				log.Error(err)
				return err
			}
			return nil
		})
	} else {
		dst := strings.Replace(task.Src, path.Dir(task.Src), task.Dst, 1)
		dstF, err := os.Stat(dst)
		if err == nil && !dstF.IsDir() {
			srcSha256, _ := mv_utils.CalFileSha256(task.Src, stat.Size())
			dstSha256, _ := mv_utils.CalFileSha256(dst, dstF.Size())
			if srcSha256 == dstSha256 {
				return
			}
		}
		err = mv_utils.MakeDirIfNotExists(dst)
		if err != nil {
			log.Error(err)
			return
		}
		err = copy(task.Src, dst, copyCycleDelay)
		if err != nil {
			log.Error(err)
			return
		}
	}

}

func getFinalDst(oriSrc, src, oriDst string) string {
	return strings.Replace(src, oriSrc, oriDst, 1)
}

func copy(src, dst string, copyCycleDelay int) (err error) {
	const BUFFER_SIZE = 1 * 1024 * 1024
	buf := make([]byte, BUFFER_SIZE)

	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		err2 := source.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}()

	mv_utils.MakeDirIfNotExists(path.Dir(dst))
	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		err2 := destination.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}()

	for {
		if stop {
			return errors.New("stop by syscall")
		}

		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// 限速
		time.Sleep(time.Second * time.Duration(copyCycleDelay))
		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
	}

	return nil
}

func waitingForTasksThreadsStop() {
	for {
		if len(workingTasks.TaskList) == 0 {
			break
		}
	}
}
