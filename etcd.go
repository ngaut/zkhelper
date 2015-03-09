package zkhelper

import (
	"errors"
	"path"
	"strings"
	"time"

	etcderr "github.com/coreos/etcd/error"
	"github.com/coreos/go-etcd/etcd"
	zk "github.com/ngaut/go-zookeeper/zk"
	log "github.com/ngaut/logging"
)

type etcdImpl struct {
	c *etcd.Client
}

func convertToZkError(err error) error {
	//todo:implementation
	if ec, ok := err.(*etcd.EtcdError); ok {
		switch ec.ErrorCode {
		case etcderr.EcodeKeyNotFound:
			return zk.ErrNoNode
		case etcderr.EcodeNotFile:
		case etcderr.EcodeNotDir:
		case etcderr.EcodeNodeExist:
			return zk.ErrNodeExists
		case etcderr.EcodeDirNotEmpty:
			return zk.ErrNotEmpty
		}
	}

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

func (e *etcdImpl) Get(key string) (data []byte, stat zk.Stat, err error) {
	resp, err := e.c.Get(key, true, false)
	if resp == nil {
		return nil, nil, convertToZkError(err)
	}

	return []byte(resp.Node.Value), nil, nil
}

func (e *etcdImpl) watch(key string, children bool) (resp *etcd.Response, stat zk.Stat, watch <-chan zk.Event, err error) {
	resp, err = e.c.Get(key, true, false)
	if resp == nil {
		return nil, nil, nil, convertToZkError(err)
	}

	ch := make(chan zk.Event, 100)

	go func(index uint64) {
		//for {
		//todo: should we use resp.Nodes's index or its children's max index
		resp, err := e.c.Watch(key, index, children, nil, nil)
		if err != nil {
			log.Warning("watch", err)
			ch <- convertToZkEvent(resp, err)
			return
		}

		ch <- convertToZkEvent(resp, err)
		index = resp.EtcdIndex
		//	}
	}(resp.Node.ModifiedIndex + 1)

	return resp, nil, ch, nil
}

func (e *etcdImpl) GetW(key string) (data []byte, stat zk.Stat, watch <-chan zk.Event, err error) {
	resp, stat, watch, err := e.watch(key, false)
	if err != nil {
		return
	}

	return []byte(resp.Node.Value), stat, watch, nil
}

func (e *etcdImpl) Children(key string) (children []string, stat zk.Stat, err error) {
	log.Debug("Children", key)
	resp, err := e.c.Get(key, true, false)
	if resp == nil {
		return nil, nil, convertToZkError(err)
	}

	log.Debugf("%+v", resp.Node)

	for _, c := range resp.Node.Nodes {
		children = append(children, path.Base(c.Key))
	}

	return
}

func (e *etcdImpl) ChildrenW(key string) (children []string, stat zk.Stat, watch <-chan zk.Event, err error) {
	resp, stat, watch, err := e.watch(key, true)
	if err != nil {
		return nil, stat, nil, convertToZkError(err)
	}

	for _, c := range resp.Node.Nodes {
		children = append(children, c.Key)
	}

	return
}

func (e *etcdImpl) Exists(key string) (exist bool, stat zk.Stat, err error) {
	_, err = e.c.Get(key, true, false)
	if err == nil {
		return true, nil, nil
	}

	if ec, ok := err.(*etcd.EtcdError); ok {
		if ec.ErrorCode == etcderr.EcodeKeyNotFound {
			return false, nil, nil
		}
	}

	return false, nil, convertToZkError(err)
}

func (e *etcdImpl) ExistsW(key string) (exist bool, stat zk.Stat, watch <-chan zk.Event, err error) {
	_, stat, watch, err = e.watch(key, false)
	if err != nil {
		return false, nil, nil, convertToZkError(err)
	}

	return true, nil, watch, nil
}

const MAX_TTL = 365 * 24 * 60 * 60

//todo:add test for keepAlive
func (e *etcdImpl) keepAlive(key string) {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			resp, err := e.c.Get(key, false, false)
			if resp == nil {
				log.Error(err)
				return
			}

			if resp.Node.Dir {
				log.Error("can not set ttl to directory")
				return
			}

			resp, err = e.c.Set(key, resp.Node.Value, uint64(resp.Node.TTL))
			if resp == nil {
				log.Error(err)
				return
			}
		}
	}()
}

func (e *etcdImpl) Create(wholekey string, value []byte, flags int32, aclv []zk.ACL) (keyCreated string, err error) {
	seq := (flags & zk.FlagSequence) != 0
	tmp := (flags & zk.FlagEphemeral) != 0
	ttl := time.Duration(MAX_TTL)
	if tmp {
		ttl = 2 * time.Second
	}

	fn := e.c.Create
	log.Info("create", wholekey)

	if seq {
		wholekey = path.Dir(wholekey)
		fn = e.c.CreateInOrder
	} else {
		for _, v := range aclv {
			if v.Perms == PERM_DIRECTORY {
				log.Info("etcdImpl:create directory", wholekey)
				fn = nil
				_, err := e.c.CreateDir(wholekey, uint64(ttl))
				if err != nil {
					log.Warning("etcdImpl:create directory", wholekey, err)
					return "", convertToZkError(err)
				}
			}
		}
	}

	if fn == nil {
		if tmp {
			e.keepAlive(wholekey)
		}
		return wholekey, nil
	}

	resp, err := fn(wholekey, string(value), uint64(ttl))
	if err != nil {
		return "", convertToZkError(err)
	}

	if tmp {
		e.keepAlive(wholekey + resp.Node.Key)
	}

	return wholekey + resp.Node.Key, nil
}

func (e *etcdImpl) Set(key string, value []byte, version int32) (stat zk.Stat, err error) {
	if version == 0 {
		return nil, errors.New("invalid version")
	}

	resp, err := e.c.Get(key, true, false)
	if resp == nil {
		return nil, convertToZkError(err)
	}

	_, err = e.c.Set(key, string(value), uint64(resp.Node.TTL))
	return nil, convertToZkError(err)
}

func (e *etcdImpl) Delete(key string, version int32) (err error) {
	//todo: handle version
	resp, err := e.c.Get(key, true, false)
	if resp == nil {
		return convertToZkError(err)
	}

	if resp.Node.Dir {
		_, err = e.c.DeleteDir(key)
	} else {
		_, err = e.c.Delete(key, false)
	}

	return convertToZkError(err)
}

func (e *etcdImpl) GetACL(key string) ([]zk.ACL, zk.Stat, error) {
	return nil, nil, nil
}

func (e *etcdImpl) SetACL(key string, aclv []zk.ACL, version int32) (zk.Stat, error) {
	return nil, nil
}

func (e *etcdImpl) Close() {
	//todo:how to implement it
}
