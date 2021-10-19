/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/27 下午4:50 _*_
**/

package main

import (
	"bytes"
	"errors"
	"fmt"
	"golang.org/x/xerrors"
	"io"
	"math"
	"move_sectors/move_common"
	"move_sectors/mv_utils"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	StatusOnWaiting = "StatusOnWaiting"
	StatusOnWorking = "StatusOnWorking"
	StatusDone      = "StatusDone"
	ProofType32G    = "32G"
	ProofType64G    = "64G"
	TreeRFormat     = "sc-02-data-tree-r-last-%d.dat"
)

type ComputersMap struct {
	CMap  map[string]Computer
	CLock *sync.Mutex
}

type TaskList struct {
	Ops   []Operation
	TLock *sync.Mutex
}

type GetSectorID interface {
	getSectorID() string
}

var _ GetSectorID = &SectorID{}

type SectorID struct {
	ID string
}

func (sc *SectorID) getSectorID() string {
	return sc.ID
}

type Operation interface {
	GetSectorID
	getInfo() interface{}
	getSrcIp() string
	getSrcPath() string
	canDo() bool
	getBestDst() (string, string, error)
	startCopy(cfg *Config, dstPath string)
	getStatus() string
	setStatus(st string)
	fullInfo(dstOri, dstIp string)
	checkIsExistedInDst(srcPaths []string, cfg *Config) bool
	checkSourceSize() ([]string, error)
	tryToFindGroupDir() (string, string, error)
}

func getOneFreeDstComputer() (*Computer, error) {
	dstComputersMapSingleton.CLock.Lock()
	defer dstComputersMapSingleton.CLock.Unlock()
	for _, cmp := range dstComputersMapSingleton.CMap {
		if cmp.CurrentThreads < cmp.LimitThread {
			return &cmp, nil
		}
	}
	return nil, errors.New(move_common.NoDstSuitableForNow)
}

func copyDir(srcDir, dst string, cfg *Config) error {
	if err := mv_utils.MakeDirIfNotExists(dst); err != nil {
		return err
	}
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if stop {
			return errors.New(move_common.StoppedBySyscall)
		}
		if info == nil || err != nil {
			return err
		}
		if path != srcDir {
			err = copying(path, dst+"/"+info.Name(), 0, cfg.Chunks)
		}
		return err
	})
	return err
}

func copying(src, dst string, singleThreadMBPS int, chunks int64) (err error) {

	if src != dst {
		//fix path with QINIU
		middlePath := dst + ".tmp"
		if err = cp(src, middlePath, singleThreadMBPS, chunks); err != nil {
			return err
		}

		if err = moveFile(middlePath, dst); err != nil {
			return err
		}
	}

	return nil
}

func cp(src, dst string, singleThreadMBPS int, chunks int64) (err error) {
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

	err = mv_utils.MakeDirIfNotExists(path.Dir(dst))
	if err != nil {
		return err
	}
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
			return errors.New(move_common.StoppedBySyscall)
		}

		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// 限速
		if singleThreadMBPS != 0 {
			sleepTime := 1000000 / int64(singleThreadMBPS)
			time.Sleep(time.Microsecond * time.Duration(sleepTime))
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
	}
	return
}

func moveFile(from, to string) error {
	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "mv", from, to) // nolint
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec mv (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}

	return nil
}

func getStandSize(proofType, path string) (int64, error) {
	var (
		baseSize  int64
		treeRSize int64
		pAuxSize  int64
	)
	switch proofType {
	case ProofType32G:
		baseSize = 34359738368
		treeRSize = 9586976
		pAuxSize = 64
	case ProofType64G:
		baseSize = 68719476736
		treeRSize = 9586976
		pAuxSize = 64
	default:
		return 0, errors.New(fmt.Sprintf("this kind of SealProofType: %d should never existed", proofType))
	}

	if strings.Contains(path, "unsealed") || strings.Contains(path, "sealed") {
		return baseSize, nil
	} else if strings.Contains(path, "tree-r") {
		return treeRSize, nil
	} else if strings.Contains(path, "p_aux") {
		return pAuxSize, nil
	} else {
		return 0, errors.New(fmt.Sprintf("this kind of path: %s should never existed", path))
	}

}

func compareSize(path string, base int64, delta int) error {
	errFormat := "wrong file size,path: %s,required size: %d, got size: %d"
	if fileStat, err := os.Stat(path); err != nil {
		return err
	} else {
		if math.Abs(float64(fileStat.Size()-base)) > float64(delta) {
			return errors.New(fmt.Sprintf(errFormat, path, base, fileStat.Size()))
		}
	}
	return nil
}

func recordCalLogIfNeed(calFunc func(string, int64, int64) (string, error), filePath string, size int64, chunks int64) (string, error) {
	since := time.Now()
	s, err := calFunc(filePath, size, chunks)
	log.Debugf("cal %s calHash cost %v, result: %s", filePath, time.Now().Sub(since), s)
	return s, err
}

func occupyThreads(dstPath, dstIp, srcIp, srcPath string) {
	srcComputersMapSingleton.CLock.Lock()
	dstComputersMapSingleton.CLock.Lock()
	// srcComputer && srcPath
	srcComputer := srcComputersMapSingleton.CMap[srcIp]
	log.Debugf("occupySrcComputer:before %d,ip %s", srcComputer.CurrentThreads, srcIp)
	srcComputer.CurrentThreads++
	// srcPath
	for idx, loc := range srcComputer.Paths {
		if srcPath == loc.Location {
			loc.CurrentThreads++
			srcComputer.Paths[idx] = loc
		}
	}
	srcComputersMapSingleton.CMap[srcIp] = srcComputer
	log.Debugf("occupySrcComputer:after %d,ip %s", srcComputersMapSingleton.CMap[srcIp].CurrentThreads, srcIp)

	// dstComputer
	dstComputer := dstComputersMapSingleton.CMap[dstIp]
	log.Debugf("occupyDstComputer:before %d,ip %s", dstComputer.CurrentThreads, dstIp)
	dstComputer.CurrentThreads++
	dstComputersMapSingleton.CMap[dstIp] = dstComputer
	log.Debugf("occupyDstComputer:after %d,ip %s", dstComputersMapSingleton.CMap[dstIp].CurrentThreads, dstIp)

	// dstPath
	for idx, p := range dstComputer.Paths {
		if p.Location == dstPath {
			log.Debugf("occupyDstPathThread:before %d,ip %s,path %s", p.CurrentThreads, dstIp, p.Location)
			p.CurrentThreads++
			dstComputer.Paths[idx] = p
			dstComputersMapSingleton.CMap[dstIp] = dstComputer
			log.Debugf("occupyDstPathThread:after %d,ip %s,path %s", p.CurrentThreads, dstIp, p.Location)
		}
	}

	srcComputersMapSingleton.CLock.Unlock()
	dstComputersMapSingleton.CLock.Unlock()
}

func freeThreads(dstPath, dstIp, srcIp, srcPath string) {
	srcComputersMapSingleton.CLock.Lock()
	dstComputersMapSingleton.CLock.Lock()
	//srcComputer
	srcComputer := srcComputersMapSingleton.CMap[srcIp]
	log.Debugf("releaseSrcComputer:before %d,ip %s", srcComputer.CurrentThreads, srcIp)
	if srcComputer.CurrentThreads < 0 {
		log.Errorf("wrong thread num,required num is bigger than 0,but %d", srcComputer.CurrentThreads)
	}
	srcComputer.CurrentThreads--
	// srcPath
	for idx, loc := range srcComputer.Paths {
		if srcPath == loc.Location {
			loc.CurrentThreads--
			srcComputer.Paths[idx] = loc
		}
	}
	srcComputersMapSingleton.CMap[srcIp] = srcComputer
	log.Debugf("releaseSrcComputer:after %d,ip %s", srcComputersMapSingleton.CMap[srcIp].CurrentThreads, srcIp)

	//dstComputer
	dstComputer := dstComputersMapSingleton.CMap[dstIp]
	log.Debugf("releaseDstComputer:before %d,ip %s", dstComputer.CurrentThreads, dstIp)
	if dstComputer.CurrentThreads < 0 {
		log.Errorf("wrong thread num,required num is bigger than 0,but %d", dstComputer.CurrentThreads)
	}
	dstComputer.CurrentThreads--
	dstComputersMapSingleton.CMap[dstIp] = dstComputer
	log.Debugf("releaseDstComputer:after %d,ip %s",
		dstComputersMapSingleton.CMap[dstIp].CurrentThreads, dstIp)

	// dstPath
	for idx, p := range dstComputer.Paths {
		if p.Location == dstPath {
			log.Debugf("releaseDstPathThread:before %d,ip %s,path %s", p.CurrentThreads, dstIp, p.Location)
			p.CurrentThreads--
			dstComputer.Paths[idx] = p
			dstComputersMapSingleton.CMap[dstIp] = dstComputer
			log.Debugf("releaseDstPathThread:after %d,ip %s,path %s", p.CurrentThreads, dstIp, p.Location)
		}
	}
	srcComputersMapSingleton.CLock.Unlock()
	dstComputersMapSingleton.CLock.Unlock()
}
