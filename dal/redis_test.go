package dal

import (
	"testing"

	"github.com/SyncSoftInc/scheduler/core"
	"github.com/czasg/go-sche"
	"github.com/stretchr/testify/assert"
)

var (
	_store sche.Store
)

func init() {
	_store = NewStoreRedis(core.RedisConfig)
}

func TestTaskLifeCycle(t *testing.T) {
	task := &sche.Task{
		Name: "Amazon1",
		Trig: "* * * * *",
		Label: map[string]string{
			"func": "callGRPC",
			"name": "job.amazon",
		},
	}
	var err error
	// Insert
	err = _store.AddTask(task)
	if !assert.NoError(t, err) {
		return
	}

	id := task.ID
	// Get
	task, err = _store.GetTaskByID(id)
	if !assert.NoError(t, err) || !assert.NotNil(t, task) {
		return
	}

	// Update
	task.Name = "task1.1"
	err = _store.UpdateTask(task)
	if !assert.NoError(t, err) {
		return
	}

	// Get
	task, err = _store.GetTaskByID(id)
	if !assert.NoError(t, err) || !assert.NotNil(t, task) || !assert.Equal(t, "task1.1", task.Name) {
		return
	}

	// Delete
	err = _store.DelTask(task)
	if !assert.NoError(t, err) {
		return
	}
}

func TestAddTask(t *testing.T) {
	task := &sche.Task{
		Name: "Amazon1",
		Trig: "*/1 * * * *",
		Label: map[string]string{
			"func": "callGRPC",
			"name": "spider.test",
			"job":  "MethodA",
		},
	}
	var err error
	// Insert
	err = _store.AddTask(task)
	if !assert.NoError(t, err) {
		return
	}
}

func TestGetRunTime(t *testing.T) {
	var err error
	// Insert
	runt, err := _store.GetNextRunTime()
	if !assert.NoError(t, err) || !assert.NotNil(t, runt) {
		return
	}
}
