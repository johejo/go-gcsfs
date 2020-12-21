package gcsfs

import (
	"context"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func init() {
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		panic(err)
	}
	time.Local = loc
}

func TestGcsFS(t *testing.T) {
	const bucketName = "gcsfs"
	ctx := context.Background()
	fakeServer := fakestorage.NewServer([]fakestorage.Object{
		readObject(t, bucketName, "gcsfs.go"),
		readObject(t, bucketName, "testdata/hello"),
	})
	fsys, err := GcsFS(ctx, fakeServer.Client(), bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if err := fstest.TestFS(fsys, "gcsfs.go", "testdata/hello"); err != nil {
		t.Fatal(err)
	}
}

func readObject(t *testing.T, bucketName, name string) fakestorage.Object {
	return fakestorage.Object{
		BucketName: bucketName,
		Name:       name,
		Content:    readFile(t, name),
	}
}

func readFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
