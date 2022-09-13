package jobgo

import (
	"context"

	"github.com/Lukiya/jobgo/dal"
	"github.com/czasg/go-sche"
	"github.com/czasg/gonal"
	"github.com/syncfuture/go/sconfig"
	"github.com/syncfuture/go/slog"
	"github.com/syncfuture/go/sredis"
	"github.com/syncfuture/host/sfasthttp"
)

type JobHost struct {
	cp        sconfig.IConfigProvider
	scheduler *sche.Scheduler
}

func NewJobHost(cp sconfig.IConfigProvider) *JobHost {
	var sedisConfig *sredis.RedisConfig
	cp.GetStruct("Redis", &sedisConfig)
	store := dal.NewStoreRedis(sedisConfig)
	return &JobHost{
		cp: cp,
		scheduler: &sche.Scheduler{
			Store: store,
		},
	}
}

func (self *JobHost) AddJob(task *sche.Task, handler gonal.Handler) {
	gonal.BindHandler(task.Label, handler)
	self.scheduler.AddTask(task)
}

func (self *JobHost) Run() error {
	server := sfasthttp.NewFHWebHost(self.cp)

	server.ServeEmbedFiles("/{filepath:*}", "wwwroot", staticFiles)

	go func() {
		slog.Fatal(self.scheduler.Start(context.Background()))
	}()

	return server.Run()
}
