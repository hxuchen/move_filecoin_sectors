package mv_utils

import (
	"errors"
	"fmt"
	logging "github.com/ipfs/go-log"
	"move_sectors/mv_common"
	"os"
	"path"
	"syscall"
	"time"
)

var log = logging.Logger("main")

func CheckDiskSize(location string, requiredSize uint64) error {
	if os.Getenv("SKIP_CHECK_DISK_SIZE") == "1" {
		return nil
	}
	var (
		err     = errors.New("CheckDiskSize time out")
		done    = make(chan struct{}, 1)
		timeOut = time.Minute * 10
	)

	go func() {
		stat := new(syscall.Statfs_t)
		err1 := syscall.Statfs(location, stat)
		defer func() {
			if errRecover := recover(); errRecover != nil {
				log.Errorf("skip panic error: %v", errRecover)
			}
			err = err1
			done <- struct{}{}
		}()
		if err1 != nil {
			return
		}
		if diskSize := stat.Blocks * uint64(stat.Bsize); diskSize < requiredSize {
			err1 = errors.New(
				fmt.Sprintf("%s, required larger than %.2f GiB avaliable, but %.2f GiB", mv_common.WrongDiskSize,
					float64(requiredSize/(1<<30)), float64(diskSize)/(1<<30)))
			return
		}
	}()
	select {
	case <-done:
	case <-time.After(timeOut):
	}
	return err
}

func GetUsedSize(path string) (uint64, error) {
	stat := new(syscall.Statfs_t)
	err := syscall.Statfs(path, stat)
	if err != nil {
		return 0, err
	}
	return ((stat.Blocks - stat.Bavail) * uint64(stat.Bsize)) >> 30, nil
}

func GetAvailableSize(path string) (uint64, error) {
	stat := new(syscall.Statfs_t)
	err := syscall.Statfs(path, stat)
	if err != nil {
		return 0, err
	}
	return (stat.Bavail * uint64(stat.Bsize)) >> 30, nil
}

func MakeDirIfNotExists(p string) error {

	// Check if parent dir exists. If not exists, create it.
	parentPath := path.Dir(p)

	_, err := os.Stat(parentPath)
	if err != nil && os.IsNotExist(err) {
		err = MakeDirIfNotExists(parentPath)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// If parent dir exists. make dir.
	err = os.Mkdir(p, 0755)
	if err != nil && os.IsExist(err) {
		return nil
	} else {
		return err
	}
}
