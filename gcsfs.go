package gcsfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

var (
	_ fs.FS         = (*gcsFS)(nil)
	_ fs.ReadFileFS = (*gcsFS)(nil)

	_ fs.File = (*file)(nil)

	_ fs.File        = (*bucketFile)(nil)
	_ fs.ReadDirFile = (*bucketFile)(nil)

	_ fs.FileInfo = (*readerObjectFileInfo)(nil)
	_ fs.DirEntry = (*readerObjectFileInfo)(nil)

	_ fs.FileInfo = (*objectFileInfo)(nil)
	_ fs.DirEntry = (*objectFileInfo)(nil)

	_ fs.FileInfo = (*bucketFileInfo)(nil)

	ctx = context.TODO()
)

func GcsFS(ctx context.Context, c *storage.Client, bucketName string) (fs.FS, error) {
	return &gcsFS{
		client:     c,
		ctx:        ctx,
		bucketName: bucketName,
	}, nil
}

type gcsFS struct {
	client     *storage.Client
	ctx        context.Context
	bucketName string
}

func (f *gcsFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}
	if name == "." {
		return &bucketFile{handle: f.client.Bucket(f.bucketName)}, nil
	}
	reader, err := f.reader(name)
	if err != nil {
		return nil, err
	}
	return &file{reader: reader, name: name}, nil
}

func (f *gcsFS) ReadFile(name string) ([]byte, error) {
	reader, err := f.reader(name)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func (f *gcsFS) reader(name string) (*storage.Reader, error) {
	return f.client.Bucket(f.bucketName).Object(name).NewReader(f.ctx)
}

type bucketFile struct {
	handle *storage.BucketHandle
}

func (f *bucketFile) ReadDir(n int) ([]fs.DirEntry, error) {
	iter := f.handle.Objects(ctx, nil)
	var size int
	if n <= 0 {
		size = iter.PageInfo().MaxSize
	} else {
		size = n
	}
	entries := make([]fs.DirEntry, 0, size)
	for {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, &objectFileInfo{
			attrs: attrs,
		})
	}
	if len(entries) == 0 {
		return entries, io.EOF
	}
	return entries, nil
}

func (f *bucketFile) Close() error {
	return nil
}

func (f *bucketFile) Stat() (fs.FileInfo, error) {
	attrs, err := f.handle.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return &bucketFileInfo{
		attrs: attrs,
	}, nil
}

func (f *bucketFile) Read(p []byte) (int, error) {
	return 0, nil
}

type bucketFileInfo struct {
	attrs *storage.BucketAttrs
}

func (fi *bucketFileInfo) Name() string {
	return fi.attrs.Name
}

func (fi *bucketFileInfo) Size() int64 {
	return 0 // TODO
}

func (fi *bucketFileInfo) Mode() fs.FileMode {
	return fs.ModeDir
}

func (fi *bucketFileInfo) ModTime() time.Time {
	return fi.attrs.Created.Truncate(time.Second)
}

func (fi *bucketFileInfo) IsDir() bool {
	return true
}

func (fi *bucketFileInfo) Sys() interface{} {
	return fi.attrs
}

type file struct {
	reader *storage.Reader
	name   string
}

func (f *file) Stat() (fs.FileInfo, error) {
	return &readerObjectFileInfo{
		attrs: f.reader.Attrs,
		name:  f.name,
	}, nil
}

func (f *file) Read(p []byte) (int, error) {
	return f.reader.Read(p)
}

func (f *file) Close() error {
	return f.reader.Close()
}

type readerObjectFileInfo struct {
	attrs storage.ReaderObjectAttrs
	name  string
}

func (fi *readerObjectFileInfo) Name() string {
	return fi.name
}

func (fi *readerObjectFileInfo) Size() int64 {
	return fi.attrs.Size
}

func (fi *readerObjectFileInfo) Mode() fs.FileMode {
	return 0
}

func (fi *readerObjectFileInfo) ModTime() time.Time {
	return fi.attrs.LastModified.Truncate(time.Second)
}

func (fi *readerObjectFileInfo) IsDir() bool {
	return false
}

func (fi *readerObjectFileInfo) Sys() interface{} {
	return fi.attrs
}

func (fi *readerObjectFileInfo) Info() (fs.FileInfo, error) {
	return fi, nil
}

func (fi *readerObjectFileInfo) Type() fs.FileMode {
	return 0
}

type objectFileInfo struct {
	attrs *storage.ObjectAttrs
}

func (fi *objectFileInfo) Name() string {
	_, file := path.Split(fi.attrs.Name)
	return file
}

func (fi *objectFileInfo) Size() int64 {
	return fi.attrs.Size
}

func (fi *objectFileInfo) Mode() fs.FileMode {
	return 0
}

func (fi *objectFileInfo) ModTime() time.Time {
	if fi.attrs.Deleted.After(fi.attrs.Created) {
		return fi.attrs.Deleted.Truncate(time.Second)
	}
	return fi.attrs.Created.Truncate(time.Second)
}

func (fi *objectFileInfo) IsDir() bool {
	return false
}

func (fi *objectFileInfo) Sys() interface{} {
	return fi.attrs
}

func (fi *objectFileInfo) Info() (fs.FileInfo, error) {
	return fi, nil
}

func (fi *objectFileInfo) Type() fs.FileMode {
	return 0
}
