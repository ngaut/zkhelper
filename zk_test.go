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
	"encoding/json"
	"flag"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

var zkAddr = flag.String("zk", ":2181", "zookeeper address")

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testElectorSuite{})

type testElectorSuite struct {
	wg sync.WaitGroup
}

type testTask struct {
	quit chan struct{}
	id   string
}

func (t *testTask) Run() error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.quit:
			return nil
		case <-ticker.C:
			println("task run", t.id)
		}
	}
}

func (t *testTask) Stop() {
	println("task stop", t.id)
}

func (t *testTask) Interrupted() bool {
	select {
	case <-t.quit:
		return true
	default:
		return false
	}
}

func (s *testElectorSuite) runTask(c *C, conn Conn, id string) *testTask {
	ze, err := CreateElectionWithContents(conn, "/zk/leader_test", map[string]interface{}{
		"id": id,
	})

	task := &testTask{
		quit: make(chan struct{}, 1),
		id:   id,
	}

	c.Assert(err, IsNil)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ze.RunTask(task)
	}()

	return task
}

func (s *testElectorSuite) TestElection(c *C) {
	c1 := s.getConn(c)
	defer c1.Close()

	c2 := s.getConn(c)
	defer c2.Close()

	task1 := s.runTask(c, c1, "1")
	time.Sleep(500 * time.Millisecond)

	task2 := s.runTask(c, c2, "2")
	time.Sleep(500 * time.Millisecond)

	s.checkLeader(c, "1")
	close(task1.quit)

	time.Sleep(500 * time.Millisecond)
	s.checkLeader(c, "2")

	close(task2.quit)

	s.wg.Wait()
}

func (s *testElectorSuite) getConn(c *C) Conn {
	conn, err := ConnectToZkWithTimeout(*zkAddr, time.Second)
	c.Assert(err, IsNil)
	return conn
}

func (s *testElectorSuite) checkLeader(c *C, id string) {
	conn := s.getConn(c)
	defer conn.Close()

	data, _, err := conn.Get("/zk/leader_test/leader")
	c.Assert(err, IsNil)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
	c.Assert(err, IsNil)

	c.Assert(m["id"], Equals, id)
}
