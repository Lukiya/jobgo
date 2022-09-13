package dal

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/czasg/go-sche"
	"github.com/go-redis/redis/v8"
	"github.com/syncfuture/go/sconv"
	"github.com/syncfuture/go/serr"
	"github.com/syncfuture/go/sredis"
	"github.com/syncfuture/go/u"
)

const (
	_hashKey = "job:Tasks"
)

func NewStoreRedis(config *sredis.RedisConfig) sche.Store {
	r := new(RedisStoreHash)
	r.client = sredis.NewClient(config)
	return r
}

type RedisStoreHash struct {
	client redis.Cmdable
}

func (x *RedisStoreHash) Todo(now time.Time) (rs []*sche.Task, err error) {
	tasks, err := x.GetTasks()
	if err != nil {
		return nil, serr.WithStack(err)
	}

	rs = make([]*sche.Task, 0, len(tasks))

	// filter
	for _, t := range tasks {
		if !t.Suspended && (t.NextRunTime.Before(now) || t.NextRunTime.Equal(now)) {
			rs = append(rs, t)
		}
	}

	return
}

func (x *RedisStoreHash) GetNextRunTime() (time.Time, error) {
	rs, err := x.GetTasks()
	if err != nil {
		return sche.MaxDateTime, serr.WithStack(err)
	}

	if len(rs) == 0 {
		return sche.MaxDateTime, nil
	}

	tasks := make([]*sche.Task, 0, len(rs))

	// filter
	for _, item := range rs {
		if !item.Suspended {
			tasks = append(tasks, item)
		}
	}

	// sort next_run_time asc
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].NextRunTime.Before(tasks[j].NextRunTime)
	})

	return tasks[0].NextRunTime, nil
}

func (x *RedisStoreHash) AddTask(task *sche.Task) error {
	jsonBytes, err := json.Marshal(task)
	if err != nil {
		return serr.WithStack(err)
	}

	err = x.client.HSet(context.Background(), _hashKey, task.ID, u.BytesToStr(jsonBytes)).Err()
	if err != nil {
		return serr.WithStack(err)
	}

	return nil
}

func (x *RedisStoreHash) UpdateTask(task *sche.Task) error {
	jsonBytes, err := json.Marshal(task)
	if err != nil {
		return serr.WithStack(err)
	}

	err = x.client.HSet(context.Background(), _hashKey, task.ID, u.BytesToStr(jsonBytes)).Err()
	if err != nil {
		return serr.WithStack(err)
	}

	return nil
}

func (x *RedisStoreHash) DelTask(task *sche.Task) error {
	err := x.client.HDel(context.Background(), _hashKey, sconv.ToString(task.ID)).Err()
	if err != nil {
		return serr.WithStack(err)
	}

	return nil
}

func (x *RedisStoreHash) GetTaskByID(id int64) (*sche.Task, error) {

	var jsonStr string
	jsonStr, err := x.client.HGet(context.Background(), _hashKey, sconv.ToString(id)).Result()
	if err != nil {
		return nil, serr.WithStack(err)
	}

	var rs *sche.Task
	err = json.Unmarshal(u.StrToBytes(jsonStr), &rs)
	if err != nil {
		return nil, serr.WithStack(err)
	}

	return rs, nil
}

func (x *RedisStoreHash) GetTasks() ([]*sche.Task, error) {
	list, err := x.client.HGetAll(context.Background(), _hashKey).Result()
	if err != nil {
		return nil, serr.WithStack(err)
	}

	rs := make([]*sche.Task, 0, len(list))

	for _, val := range list {
		var dto *sche.Task
		err = json.Unmarshal(u.StrToBytes(val), &dto)
		if err != nil {
			return nil, serr.WithStack(err)
		}

		rs = append(rs, dto)
	}

	return rs, nil
}

// func (x *RedisStoreHash) getID() (int64, error) {
// 	// count, err := x.client.HLen(context.Background(), _hashKey).Result()
// 	rs, err := x.client.ZRevRangeWithScores(context.Background(), _hashKey, 0, 0).Result()
// if err != nil {
// 	return 0,serr.WithStack(err)
// }

// 	if rs != nil && len(rs) > 0 {
// 		return int64(rs[0].Score) + 1, nil
// 	}

// 	return 0, nil
// }
