package atomic

import (
	"io/ioutil"
	"os"
)

type File struct {
	tf   *os.File
	path string
}

func Open(path string) (*File, error) {
	tf, err := ioutil.TempFile("", "go-peers-atomic-file-*")
	if err != nil {
		return nil, err
	}
	return &File{tf: tf, path: path}, nil
}

func (f *File) Write(d []byte) (int, error) { return f.tf.Write(d) }
func (f *File) Close() error {
	err := f.tf.Close()
	if err != nil {
		os.Remove(f.tf.Name())
		return err
	}
	return os.Rename(f.tf.Name(), f.path)
}
