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
	Tasks map[string]CpTask
	WLock *sync.Mutex
}

var workingTasks = WorkingTasks{
	Tasks: make(map[string]CpTask, 0),
	WLock: new(sync.Mutex),
}

func start(cfg *Config) {
	stopSignal := make(chan os.Signal, 2)
	signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
ForTasks:
	for _, task := range cfg.CpTasks {
		if stop {
			break
		}
		srcComputer := computersMapSingleton.CMap[task.SrcIp]
		dstComputer := computersMapSingleton.CMap[task.DstIp]
		select {
		case <-canGo(&srcComputer, &dstComputer):
			// thread ++
			workingTasks.Tasks[task.Src] = task
			go copyGo(task, cfg.SingleThreadMBPS, &srcComputer, &dstComputer)
			computersMapSingleton.CLock.Lock()
			srcComputer.CurrentThreads++
			dstComputer.CurrentThreads++
			computersMapSingleton.CMap[task.SrcIp] = srcComputer
			computersMapSingleton.CMap[task.DstIp] = dstComputer
			log.Infof("src:%s, current threads:%d,dst:%s, current threads:%d", task.SrcIp, srcComputer.CurrentThreads, task.DstIp, dstComputer.CurrentThreads)
			computersMapSingleton.CLock.Unlock()
			time.Sleep(time.Second * 1)
		case <-stopSignal:
			break ForTasks
		}
	}
	waitingForTasksThreadsStop()
}

func initializeComputerMapSingleton(cfg *Config) error {
	for _, v := range cfg.Computers {
		if v.Ip == "" || v.BandWidth == 0 {
			return errors.New("invalid computer ip or BandWidth,please check the config")
		}
		if _, ok := computersMapSingleton.CMap[v.Ip]; !ok {
			v.LimitThread = calThreadLimit(v.BandWidth, cfg.SingleThreadMBPS)
			computersMapSingleton.CMap[v.Ip] = v

		} else {
			return errors.New("double computer ip,please check the config")
		}
	}
	return nil
}

func copyGo(task CpTask, singleThreadMBPS int, srcComputer, dstComputer *Computer) {
	log.Infof("start to do task %v", task)
	stat, err := os.Stat(task.Src)
	if err != nil {
		log.Error(err)
		return
	}
	if stat.IsDir() {
		err = filepath.Walk(task.Src, func(path string, srcF os.FileInfo, err error) error {
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
			err = copy(path, dst, singleThreadMBPS)
			if err != nil {
				log.Error(err)
				return err
			}
			return nil
		})
		workingTasks.WLock.Lock()

		computersMapSingleton.CLock.Lock()
		srcComputer.CurrentThreads--
		dstComputer.CurrentThreads--
		computersMapSingleton.CMap[task.SrcIp] = *srcComputer
		computersMapSingleton.CMap[task.DstIp] = *dstComputer

		delete(workingTasks.Tasks, task.Src)
		log.Info("task remain: %d", len(workingTasks.Tasks))
		workingTasks.WLock.Unlock()

		log.Infof("src:%s, current threads:%d,dst:%s, current threads:%d", task.SrcIp, srcComputer.CurrentThreads, task.DstIp, dstComputer.CurrentThreads)
		computersMapSingleton.CLock.Unlock()
		log.Infof("task: %v done", task)
		if err != nil {
			log.Errorf("task %v done with error: %v", task, err)
		}
	} else {
		dst := strings.Replace(task.Src, path.Dir(task.Src), task.Dst, 1)
		dstF, err := os.Stat(dst)
		if err == nil && !dstF.IsDir() {
			srcSha256, _ := mv_utils.CalFileSha256(task.Src, stat.Size())
			dstSha256, _ := mv_utils.CalFileSha256(dst, dstF.Size())
			if srcSha256 == dstSha256 {
				log.Infof("src file: %s existed in dst %s", task.Src, task.Dst)
				return
			}
		}
		err = mv_utils.MakeDirIfNotExists(path.Dir(dst))
		if err != nil {
			log.Error(err)
			return
		}
		err = copy(task.Src, dst, singleThreadMBPS)

		computersMapSingleton.CLock.Lock()
		srcComputer.CurrentThreads--
		dstComputer.CurrentThreads--
		computersMapSingleton.CMap[task.SrcIp] = *srcComputer
		computersMapSingleton.CMap[task.DstIp] = *dstComputer
		computersMapSingleton.CLock.Unlock()

		workingTasks.WLock.Lock()
		delete(workingTasks.Tasks, task.Src)
		log.Info("task remain: %d", len(workingTasks.Tasks))
		workingTasks.WLock.Unlock()

		if err != nil {
			log.Errorf("task %v done with error: %v", task, err)
			return
		}
	}

}

func getFinalDst(oriSrc, src, oriDst string) string {
	return strings.Replace(src, oriSrc, oriDst, 1)
}

func copy(src, dst string, singleThreadMBPS int) (err error) {
	const BufferSize = 1 * 1024 * 1024
	buf := make([]byte, BufferSize)

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
	readed := 0
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

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}

		// 限速
		readed += len(buf)
		if readed >= (singleThreadMBPS << 20) {
			readed = 0
			time.Sleep(time.Second * 1)
		}
	}

	return
}

func waitingForTasksThreadsStop() {
	for {
		if len(workingTasks.Tasks) == 0 {
			break
		}
		time.Sleep(time.Second * 1)
	}
}
