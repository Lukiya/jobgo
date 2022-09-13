package jobgo_test

import (
	"context"
	"testing"

	"github.com/Lukiya/jobgo"
	"github.com/czasg/go-sche"
	"github.com/czasg/gonal"
	"github.com/syncfuture/go/sconfig"
	"github.com/syncfuture/go/slog"
)

func Test(t *testing.T) {
	cp := sconfig.NewJsonConfigProvider()
	h := jobgo.NewJobHost(cp)

	h.AddJob(&sche.Task{
		ID:    0,
		Name:  "Test1",
		Label: map[string]string{"func": "handler1"},
		Trig:  "* * * * *",
	}, handler1)

	h.AddJob(&sche.Task{
		ID:    1,
		Name:  "Test2",
		Label: map[string]string{"func": "handler2"},
		Trig:  "*/5 * * * *",
	}, handler2)

	slog.Fatal(h.Run())
}

func handler1(ctx context.Context, labels gonal.Labels, data []byte) {
yghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghghgh	slog.Info("1")
}

func handler2(ctx context.Context, labels gonal.Labels, data []byte) {
	slog.Info("2")
}
