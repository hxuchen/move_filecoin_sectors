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
	"path"
	"path/filepath"
	"strings"
)

func start(cfg *Config) {
	for _, task := range cfg.CpTasks {
		copyGo(task)
	}
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

func copyGo(task CpTask) error {
	err := filepath.Walk(task.Src, func(path string, srcF os.FileInfo, err error) error {
		if err != nil || srcF == nil {
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
			return err
		}
		err = copy(path, dst)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func getFinalDst(oriSrc, src, oriDst string) string {
	return strings.Replace(src, oriSrc, oriDst, 1)
}

func copy(src, dst string) (err error) {

	if src == dst {
		return nil
	}
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

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
	}

	return nil
}
