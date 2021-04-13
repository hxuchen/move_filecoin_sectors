/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"bufio"
	"fmt"
	"io"
	"move_sectors/mv_common"
	"move_sectors/mv_utils"
	"os"
	"path"
	"time"
)

func startCopy() {

}

func copy(src, dst string, SpeedMod bool) (err error) {

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

	utils_abm.MakeDirIfNotExists(path.Dir(dst))
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

		if limitSpeed {
			time.Sleep(time.Millisecond * 10)
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
	}

	return nil
}

// initialize src paths
func initializeSrcPathList(srcPathFile string) (uint64, error) {
	fi, err := os.Open(srcPathFile)
	if err != nil {
		return 0, err
	}
	defer fi.Close()
	var totalUsage uint64
	br := bufio.NewReader(fi)
	for {
		singlePath, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		if usage, err := mv_utils.GetUsedSize(string(singlePath)); err != nil {
			return 0, err
		} else {
			srcPathList = append(srcPathList,
				mv_common.SrcFiles{
					Path:     string(singlePath),
					Usage:    usage,
					SpeedMod: mv_common.FastMod,
				})
			totalUsage += usage
		}
	}
	return totalUsage, nil
}
