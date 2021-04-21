/**
 _*_ @Author: IronHuang _*_
 _*_ @blog:https://www.dvpos.com/ _*_
 _*_ @Date: 2021/4/15 上午9:41 _*_
**/

package mv_utils

import (
	"bufio"
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
	return fileSha256(raw)
}

func MakeCalData(filePath string, size int64) ([]byte, error) {
	const BUFFER_SIZE = 4 * 1024
	var sample []byte
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if size <= BUFFER_SIZE*50 {
		reader := bufio.NewReader(file)
		sample, err = ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	} else {
		buf := make([]byte, BUFFER_SIZE)
		chunk := size / 50
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
			if passed := point + BUFFER_SIZE; passed < size && point+chunk >= size {
				bufTail := make([]byte, BUFFER_SIZE)
				file.Seek(size-BUFFER_SIZE, 0)
				n, err := file.Read(bufTail)
				if err != nil && err != io.EOF {
					return nil, err
				}
				if n == 0 {
					break
				}
				buf = append(buf, bufTail...)
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
