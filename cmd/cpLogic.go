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
	"io/ioutil"
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
	lenTasks := len(cfg.CpTasks)
ForTasks:
	for idx, task := range cfg.CpTasks {
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

			addThread(srcComputer, dstComputer, task)

			time.Sleep(time.Second * 1)

			if idx == lenTasks-1 {
				waitingForAllTasksDone()
				break ForTasks
			}

		case <-stopSignal:
			break ForTasks
		}
	}
	waitingForAllTasksDone()
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
		var fileList = make([]string, 0)
		files, err := GetAllFile(task.Src, fileList)
		for _, file := range files {
			if stop {
				break
			}
			dst := getFinalDst(task.Src, file, task.Dst)
			dstF, errFor := os.Stat(dst)
			if errFor == nil && !dstF.IsDir() {
				srcF, _ := os.Stat(file)
				if dstF.Size() == srcF.Size() {
					now := time.Now()
					srcSha256, _ := mv_utils.CalFileSha256(file, srcF.Size())
					dstSha256, _ := mv_utils.CalFileSha256(dst, dstF.Size())
					//if string(srcSha256) == string(dstSha256) {
					//	log.Infof("src file: %s already existed in dst %s,task done,calHash cost %v", file, dst, time.Now().Sub(now))
					//	continue
					//}
					sum := byte(0)
					for idx, b := range srcSha256 {
						sum += b ^ dstSha256[idx]
					}
					if sum != 0 {
						log.Infof("src file: %s already existed in dst %s,task done,calHash cost %v", file, dst, time.Now().Sub(now))
						continue
					}
				}
			}
			errFor = mv_utils.MakeDirIfNotExists(filepath.Dir(dst))
			if errFor != nil {
				log.Error(errFor)
				err = errFor
				break
			}
			errFor = copy(file, dst, singleThreadMBPS)
			if errFor != nil {
				log.Error(errFor)
				err = errFor
				break
			}
		}
		minusThread(srcComputer, dstComputer, task)
		delWorkingTasks(task)
		if err != nil {
			log.Errorf("task %v done with error: %v", task, err)
		} else {
			log.Infof("task: %v done", task)
		}
	} else {
		if stat.Size() == 0 {
			minusThread(srcComputer, dstComputer, task)
			delWorkingTasks(task)
			return
		}
		dst := strings.Replace(task.Src, path.Dir(task.Src), task.Dst, 1)
		dstF, err := os.Stat(dst)
		if err == nil && !dstF.IsDir() {
			if dstF.Size() == stat.Size() {
				srcSha256, _ := mv_utils.CalFileSha256(task.Src, stat.Size())
				dstSha256, _ := mv_utils.CalFileSha256(dst, dstF.Size())
				//now := time.Now()
				//if string(srcSha256) == string(dstSha256) {
				//	minusThread(srcComputer, dstComputer, task)
				//	delWorkingTasks(task)
				//	log.Infof("src file: %s already existed in dst %s,task done,calHash cost %v", task.Src, dst, time.Now().Sub(now))
				//	return
				//}
				now := time.Now()
				sum := byte(0)
				for idx, b := range srcSha256 {
					if b^dstSha256[idx] == 0 {
						sum += b ^ dstSha256[idx]
					}
				}
				if sum != 0 {
					log.Infof("src file: %s already existed in dst %s,task done,calHash cost %v", task.Src, dst, time.Now().Sub(now))
					minusThread(srcComputer, dstComputer, task)
					delWorkingTasks(task)
					return
				}
			}
		}
		err = mv_utils.MakeDirIfNotExists(path.Dir(dst))
		if err != nil {
			log.Error(err)
			return
		}
		err = copy(task.Src, dst, singleThreadMBPS)
		if err != nil {
			log.Errorf("task %v done with error: %v", task, err)
		}

		minusThread(srcComputer, dstComputer, task)

		delWorkingTasks(task)

		if err == nil {
			log.Infof("task: %v done", task)
		}
		return
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

		// 限速
		readed += len(buf)
		if readed >= (singleThreadMBPS << 20) {
			readed = 0
			time.Sleep(time.Second * 1)
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}

	}

	return
}

func waitingForAllTasksDone() {
	for {
		if len(workingTasks.Tasks) == 0 {
			break
		}
		time.Sleep(time.Second * 1)
	}
}

func addThread(srcComputer, dstComputer Computer, task CpTask) {
	computersMapSingleton.CLock.Lock()
	defer computersMapSingleton.CLock.Unlock()
	srcComputer.CurrentThreads++
	dstComputer.CurrentThreads++
	computersMapSingleton.CMap[task.SrcIp] = srcComputer
	computersMapSingleton.CMap[task.DstIp] = dstComputer
	log.Infof("src:%s, current threads:%d,dst:%s, current threads:%d", task.SrcIp, srcComputer.CurrentThreads, task.DstIp, dstComputer.CurrentThreads)
}

func minusThread(srcComputer, dstComputer *Computer, task CpTask) {
	computersMapSingleton.CLock.Lock()
	defer computersMapSingleton.CLock.Unlock()
	if srcComputer.CurrentThreads > 0 {
		srcComputer.CurrentThreads--
	}
	if dstComputer.CurrentThreads > 0 {
		dstComputer.CurrentThreads--
	}
	log.Infof("src:%s, current threads:%d,dst:%s, current threads:%d", task.SrcIp, srcComputer.CurrentThreads, task.DstIp, dstComputer.CurrentThreads)
	computersMapSingleton.CMap[task.SrcIp] = *srcComputer
	computersMapSingleton.CMap[task.DstIp] = *dstComputer
}

func delWorkingTasks(task CpTask) {
	workingTasks.WLock.Lock()
	defer workingTasks.WLock.Unlock()
	delete(workingTasks.Tasks, task.Src)
	log.Infof("working task remain: %d", len(workingTasks.Tasks))
}

func GetAllFile(src string, fileList []string) ([]string, error) {
	slash := filepath.FromSlash(src)
	dir, err := ioutil.ReadDir(slash)
	if err != nil {
		return fileList, err
	}
	for _, fi := range dir {
		// 如果还是文件夹
		if fi.IsDir() {
			fullDir := filepath.Join(slash, fi.Name())
			// 继续遍历
			fileList, err = GetAllFile(fullDir, fileList)
			if err != nil {
				return fileList, err
			}
		} else {
			fullName := filepath.Join(slash, fi.Name())
			fileList = append(fileList, fullName)
		}
	}
	return fileList, nil
}
