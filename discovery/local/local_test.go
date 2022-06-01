package local

import (
	"context"
	"log"
	"os"
	"testing"
)

func TestLocal(t *testing.T) {
	path := GetBinarySpecificPath()
	defer func() {
		os.RemoveAll(path)
	}()
	t.Logf("file: %s", path)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()
	d1, err := New(ctx, path, "d1", log.Default())
	if err != nil {
		t.Fatal(err)
	}
	d2, err := New(ctx, path, "d2", log.Default())
	if err != nil {
		t.Fatal(err)
	}
	d1.Discover(ctx)
	d2.Discover(ctx)
	if msg := d1.Discover(ctx); msg != "d2" {
		t.Errorf("expected 'd2', got '%s'", msg)
	}
	if msg := d2.Discover(ctx); msg != "d1" {
		t.Errorf("expected 'd1', got '%s'", msg)
	}
}
