/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/15 上午9:41 _*_
**/

package mv_utils

import (
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
)

func CalFileSha256(filePath string, size int64) (string, error) {
	raw, err := MakeCalData(filePath, size)
	if err != nil {
		return "", err
	}
	return fileMd5(raw)
}

func MakeCalData(filePath string, size int64) ([]byte, error) {
	const BUFFER_SIZE = 1024 * 4
	var sample []byte
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if size <= BUFFER_SIZE*20 {
		reader := bufio.NewReader(file)
		sample, err = ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	} else {
		buf := make([]byte, BUFFER_SIZE)
		chunk := size / 20
		for point := int64(0); point < size; point += chunk {
			file.Seek(point, 0)
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				return nil, err
			}
			if n == 0 {
				break
			}
			// read the tail of file
			if point+BUFFER_SIZE < size && point+chunk >= size {
				bufTail := make([]byte, BUFFER_SIZE)
				if remain := size - (point + BUFFER_SIZE); remain < BUFFER_SIZE {
					bufTail = make([]byte, remain)
				}
				file.Seek(size-int64(len(bufTail)), 0)
				num, err := file.Read(bufTail)
				if err != nil && err != io.EOF {
					return nil, err
				}
				if num != 0 {
					buf = append(buf, bufTail...)
				}
			}

			sample = append(sample, buf...)
		}
	}
	return sample, nil
}

func fileSha256(data []byte) (string, error) {
	_sha256 := sha256.New()
	_sha256.Write(data)
	return hex.EncodeToString(_sha256.Sum([]byte(""))), nil
}

func fileSha1(data []byte) (string, error) {
	_sha1 := sha1.New()
	_sha1.Write(data)
	return hex.EncodeToString(_sha1.Sum([]byte(""))), nil
}

func fileMd5(data []byte) (string, error) {
	_md5 := md5.New()
	_md5.Write(data)
	return hex.EncodeToString(_md5.Sum([]byte(""))), nil
}
