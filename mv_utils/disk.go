package mv_utils

import (
	"os"
	"path"
)

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
