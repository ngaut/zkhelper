// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package zkhelper

import (
	"sync"
	"testing"
	"time"
)

type testTask struct {
	quit chan struct{}
}

func (t *testTask) Run() error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.quit:
			return nil
		case <-ticker.C:
			println("task run")
		}
	}
}

func (t *testTask) Stop() {
	println("task stop")
}

func (t *testTask) Interrupted() bool {
	select {
	case <-t.quit:
		return true
	default:
		return false
	}
}

func TestElection(t *testing.T) {
	conn := NewConn()
	defer conn.Close()

	contents := map[string]interface{}{
		"addr": "127.0.0.1:1234",
	}

	ze, err := CreateElectionWithContents(conn, "/zk", contents)
	if err != nil {
		t.Fatal(err)
	}

	task := &testTask{
		quit: make(chan struct{}, 1),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ze.RunTask(task)
	}()

	time.Sleep(100 * time.Millisecond)
	close(task.quit)

	wg.Wait()
}
