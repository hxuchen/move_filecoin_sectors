/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/13 下午9:32 _*_
**/

package main

import (
	"fmt"
	"io"
	"move_sectors/mv_common"
	"os"
	"path"
	"time"
)

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
