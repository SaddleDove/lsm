package fsx

import (
	"os"
	"path/filepath"
)

type OSFS struct {
	root  string
	stats Stats
}

func NewOSFS(root string) (*OSFS, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	return &OSFS{root: root}, nil
}

func (o *OSFS) Stats() *Stats { return &o.stats }

func (o *OSFS) resolve(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(o.root, name)
}

func (o *OSFS) Create(name string) (File, error) {
	return o.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

func (o *OSFS) Open(name string) (File, error) {
	return o.OpenFile(name, os.O_RDONLY, 0)
}

func (o *OSFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	p := o.resolve(name)
	if flag&os.O_CREATE != 0 {
		o.stats.FilesCreated.Add(1)
	}
	f, err := os.OpenFile(p, flag, perm)
	if err != nil {
		return nil, err
	}
	o.stats.OpCount.Add(1)
	return &osFile{File: f, fs: o, name: name}, nil
}

func (o *OSFS) Remove(name string) error {
	err := os.Remove(o.resolve(name))
	if err == nil {
		o.stats.FilesRemoved.Add(1)
		o.stats.OpCount.Add(1)
	}
	return err
}

func (o *OSFS) Rename(old, new string) error {
	err := os.Rename(o.resolve(old), o.resolve(new))
	if err == nil {
		o.stats.Renames.Add(1)
		o.stats.OpCount.Add(1)
	}
	return err
}

func (o *OSFS) MkdirAll(name string, perm os.FileMode) error {
	return os.MkdirAll(o.resolve(name), perm)
}

func (o *OSFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(o.resolve(name))
}

func (o *OSFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(o.resolve(name))
}

func (o *OSFS) SyncDir(name string) error {
	d, err := os.Open(o.resolve(name))
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return err
	}
	o.stats.DirFsyncs.Add(1)
	o.stats.OpCount.Add(1)
	return nil
}

type osFile struct {
	*os.File
	fs   *OSFS
	name string
}

func (f *osFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.fs.stats.BytesRead.Add(int64(n))
	f.fs.stats.OpCount.Add(1)
	return n, err
}

func (f *osFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.fs.stats.BytesRead.Add(int64(n))
	f.fs.stats.OpCount.Add(1)
	return n, err
}

func (f *osFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.fs.stats.BytesWritten.Add(int64(n))
	f.fs.stats.OpCount.Add(1)
	return n, err
}

func (f *osFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.fs.stats.BytesWritten.Add(int64(n))
	f.fs.stats.OpCount.Add(1)
	return n, err
}

func (f *osFile) Sync() error {
	info, err := f.File.Stat()
	if err == nil {
		f.fs.stats.BytesSynced.Add(info.Size())
	}
	if err := f.File.Sync(); err != nil {
		return err
	}
	f.fs.stats.Fsyncs.Add(1)
	f.fs.stats.OpCount.Add(1)
	return nil
}
