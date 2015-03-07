package zkhelper

import (
	"errors"
	etcderr "github.com/coreos/etcd/error"
	"github.com/coreos/go-etcd/etcd"
	zk "github.com/ngaut/go-zookeeper/zk"
	log "github.com/ngaut/logging"
	"path"
	"strings"
	"time"
)

type etcdImpl struct {
	c *etcd.Client
}

func convertToZkError(err error) error {
	//todo:implementation
	return err
}

func convertToZkEvent(resp *etcd.Response, err error) zk.Event {
	return zk.Event{}
}

func NewEtcdConn(zkAddr string) (Conn, error) {
	e := &etcdImpl{c: etcd.NewClient(strings.Split(zkAddr, ","))}
	if e == nil {
		return nil, errors.New("unknown error")
	}
	return e, nil
}

func (e *etcdImpl) Get(path string) (data []byte, stat zk.Stat, err error) {
	resp, err := e.c.Get(path, true, false)
	if resp == nil {
		return nil, nil, convertToZkError(err)
	}

	return []byte(resp.Node.Value), nil, nil
}

func (e *etcdImpl) watch(path string, children bool) (resp *etcd.Response, stat zk.Stat, watch <-chan zk.Event, err error) {
	resp, err = e.c.Get(path, true, false)
	if resp == nil {
		return nil, nil, nil, convertToZkError(err)
	}

	ch := make(chan zk.Event, 100)

	go func(index uint64) {
		for {
			//todo: should we use resp.Nodes's index or its children's max index
			resp, err := e.c.Watch(path, index, children, nil, nil)
			if err != nil {
				event := convertToZkEvent(resp, err)
				ch <- event
				return
			}

			index = resp.EtcdIndex
		}
	}(resp.Node.ModifiedIndex + 1)

	return resp, nil, ch, nil
}

func (e *etcdImpl) GetW(path string) (data []byte, stat zk.Stat, watch <-chan zk.Event, err error) {
	resp, stat, watch, err := e.watch(path, false)
	if err != nil {
		return
	}

	return []byte(resp.Node.Value), stat, watch, nil
}

func (e *etcdImpl) Children(path string) (children []string, stat zk.Stat, err error) {
	resp, err := e.c.Get(path, true, false)
	if resp == nil {
		return nil, nil, convertToZkError(err)
	}

	for _, c := range resp.Node.Nodes {
		children = append(children, c.Key)
	}

	return
}

func (e *etcdImpl) ChildrenW(path string) (children []string, stat zk.Stat, watch <-chan zk.Event, err error) {
	resp, stat, watch, err := e.watch(path, true)
	if err != nil {
		return nil, stat, nil, convertToZkError(err)
	}

	for _, c := range resp.Node.Nodes {
		children = append(children, c.Key)
	}

	return
}

func (e *etcdImpl) Exists(path string) (exist bool, stat zk.Stat, err error) {
	_, err = e.c.Get(path, true, false)
	if err == nil {
		return true, nil, nil
	}

	if ec, ok := err.(*etcderr.Error); ok {
		if ec.ErrorCode == etcderr.EcodeKeyNotFound {
			return false, nil, nil
		}
	}

	return false, nil, convertToZkError(err)
}

func (e *etcdImpl) ExistsW(path string) (exist bool, stat zk.Stat, watch <-chan zk.Event, err error) {
	_, stat, watch, err = e.watch(path, false)
	if err != nil {
		return false, nil, nil, convertToZkError(err)
	}

	return true, nil, watch, nil
}

const MAX_TTL = 365 * 24 * 60 * 60

func (e *etcdImpl) keepAlive(path string) {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			resp, err := e.c.Get(path, true, false)
			if resp == nil {
				log.Error(err)
				return
			}

			resp, err = e.c.Set(path, resp.Node.Value, uint64(resp.Node.TTL))
			if resp == nil {
				log.Error(err)
				return
			}
		}
	}()
}

func (e *etcdImpl) Create(wholepath string, value []byte, flags int32, aclv []zk.ACL) (pathCreated string, err error) {
	seq := (flags & zk.FlagSequence) != 0
	tmp := (flags & zk.FlagEphemeral) != 0
	ttl := time.Duration(MAX_TTL)
	if tmp {
		ttl = 2 * time.Second
	}

	fn := e.c.Create
	log.Info("create", wholepath)

	if seq {
		wholepath = path.Dir(wholepath)
		fn = e.c.CreateInOrder
	} else {
		for _, v := range aclv {
			if v.Perms == PERM_DIRECTORY {
				log.Info("etcdImpl:create directory", wholepath)
				fn = nil
				_, err := e.c.CreateDir(wholepath, uint64(ttl))
				if err != nil {
					log.Warning("etcdImpl:create directory", wholepath, err)
					return "", convertToZkError(err)
				}
			}
		}
	}

	if fn == nil {
		if tmp {
			e.keepAlive(wholepath)
		}
		return wholepath, nil
	}

	resp, err := fn(wholepath, string(value), uint64(ttl))
	if err != nil {
		return "", convertToZkError(err)
	}

	if tmp {
		e.keepAlive(wholepath + resp.Node.Key)
	}

	return wholepath + resp.Node.Key, nil
}

func (e *etcdImpl) Set(path string, value []byte, version int32) (stat zk.Stat, err error) {
	resp, err := e.c.Get(path, true, false)
	if resp == nil {
		return nil, convertToZkError(err)
	}

	_, err = e.c.Set(path, string(value), uint64(resp.Node.TTL))
	return nil, convertToZkError(err)
}

func (e *etcdImpl) Delete(path string, version int32) (err error) {
	//todo: handle version
	resp, err := e.c.Get(path, true, false)
	if resp == nil {
		return convertToZkError(err)
	}

	if resp.Node.Dir {
		_, err = e.c.DeleteDir(path)
	} else {
		_, err = e.c.Delete(path, false)
	}

	return convertToZkError(err)
}

func (e *etcdImpl) GetACL(path string) ([]zk.ACL, zk.Stat, error) {
	panic("not support")
}

func (e *etcdImpl) SetACL(path string, aclv []zk.ACL, version int32) (zk.Stat, error) {
	panic("not support")
}

func (e *etcdImpl) Close() {
	//todo:how to implement it
}
