package dal

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/czasg/go-sche"
	"github.com/go-redis/redis/v8"
	"github.com/syncfuture/go/sconv"
	"github.com/syncfuture/go/sredis"
	"github.com/syncfuture/go/u"
)

const (
	_zsetKey = "job:Tasks"
)

func NewStoreRedisZSet(ctx context.Context, config *sredis.RedisConfig) sche.Store {
	r := new(RedisStoreZSet)
	r.client = sredis.NewClient(config)
	return r
}

type RedisStoreZSet struct {
	client redis.Cmdable
}

func (x *RedisStoreZSet) Todo(now time.Time) (rs []*sche.Task, err error) {
	rs = make([]*sche.Task, 0)

	tasks, err := x.GetTasks()
	if u.LogError(err) {
		return
	}

	// filter
	for _, t := range tasks {
		if (t.NextRunTime.Before(now) || t.NextRunTime.Equal(now)) && !t.Suspended {
			rs = append(rs, t)
		}
	}

	return
}

func (x *RedisStoreZSet) GetNextRunTime() (time.Time, error) {
	tasks := make([]*sche.Task, 0)

	rs, err := x.GetTasks()
	if u.LogError(err) {
		return sche.MaxDateTime, err
	}

	if len(tasks) == 0 {
		return sche.MaxDateTime, nil
	}

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

func (x *RedisStoreZSet) AddTask(task *sche.Task) error {
	id, err := x.getID()
	if u.LogError(err) {
		return err
	}

	task.ID = id
	jsonBytes, err := json.Marshal(task)
	if u.LogError(err) {
		return err
	}

	x.client.ZAdd(context.Background(), _zsetKey, &redis.Z{
		Score:  float64(task.ID),
		Member: u.BytesToStr(jsonBytes),
	})
	return nil
}

func (x *RedisStoreZSet) UpdateTask(task *sche.Task) error {
	jsonBytes, err := json.Marshal(task)
	if u.LogError(err) {
		return err
	}

	err = x.DelTask(task)
	if u.LogError(err) {
		return err
	}

	// x.client.HSet(context.Background(), _zsetKey, task.ID, u.BytesToStr(jsonBytes))
	x.client.ZAdd(context.Background(), _zsetKey, &redis.Z{
		Score:  float64(task.ID),
		Member: u.BytesToStr(jsonBytes),
	})
	return nil
}

func (x *RedisStoreZSet) DelTask(task *sche.Task) error {
	// _, err := x.client.HDel(context.Background(), _zsetKey, sconv.ToString(task.ID)).Result()
	_, err := x.client.ZRemRangeByScore(context.Background(), _zsetKey, sconv.ToString(task.ID), sconv.ToString(task.ID)).Result()
	u.LogError(err)
	return err
}

func (x *RedisStoreZSet) GetTaskByID(id int64) (*sche.Task, error) {
	rs := new(sche.Task)

	var jsonStr string
	// jsonStr, err := x.client.HGet(context.Background(), _zsetKey, strconv.Itoa(int(id))).Result()
	jsonRs, err := x.client.ZRangeByScore(context.Background(), _zsetKey, &redis.ZRangeBy{
		Min: sconv.ToString(id), Max: sconv.ToString(id), Offset: 0, Count: 1,
	}).Result()
	u.LogError(err)

	if jsonRs != nil && len(jsonRs) > 0 {
		jsonStr = jsonRs[0]
	}

	err = json.Unmarshal(u.StrToBytes(jsonStr), &rs)
	u.LogError(err)
	return rs, err
}

func (x *RedisStoreZSet) GetTasks() ([]*sche.Task, error) {
	rs := make([]*sche.Task, 0)

	// list, err := x.client.HGetAll(context.Background(), _zsetKey).Result()
	list, err := x.client.ZRange(context.Background(), _zsetKey, 0, -1).Result()
	u.LogError(err)

	for _, val := range list {
		dto := new(sche.Task)
		err = json.Unmarshal(u.StrToBytes(val), dto)
		u.LogError(err)

		rs = append(rs, dto)
	}

	return rs, err
}

func (x *RedisStoreZSet) getID() (int64, error) {
	// count, err := x.client.HLen(context.Background(), _zsetKey).Result()
	rs, err := x.client.ZRevRangeWithScores(context.Background(), _zsetKey, 0, 0).Result()
	if u.LogError(err) {
		return 0, err
	}

	if rs != nil && len(rs) > 0 {
		return int64(rs[0].Score) + 1, nil
	}

	return 0, nil
}
