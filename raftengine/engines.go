// Copyright 2021-present PingCAP, Inc.
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

package raftengine

import (
	"encoding/binary"
	"fmt"
	"github.com/dgryski/go-farm"
	"github.com/pingcap/kvproto/pkg/eraftpb"
)

type WriteBatch struct {
	index           uint64
	raftWriteBatchs []*writeBatch
}

func NewRegionWriteBatch(regionID uint64) *WriteBatch {
	return NewWriteBatch(regionID, 1)
}

func NewWriteBatch(index uint64, count int) *WriteBatch {
	bs := &WriteBatch{
		index:           index,
		raftWriteBatchs: make([]*writeBatch, count),
	}
	for i := 0; i < count; i++ {
		bs.raftWriteBatchs[i] = newWriteBatch()
	}
	return bs
}

func (b *WriteBatch) getWriteBatch(regionID uint64) *writeBatch {
	idx := hashRegionID(regionID) % uint64(len(b.raftWriteBatchs))
	return b.raftWriteBatchs[idx]
}

func (b *WriteBatch) AppendRaftLog(regionID uint64, entry *eraftpb.Entry) {
	b.getWriteBatch(regionID).AppendRaftLog(regionID, entry)
}

func (b *WriteBatch) TruncateRaftLog(regionID, index uint64) {
	b.getWriteBatch(regionID).TruncateRaftLog(regionID, index)
}

func (b *WriteBatch) SetState(regionID uint64, key, val []byte) {
	b.getWriteBatch(regionID).SetState(regionID, key, val)
}

func (b *WriteBatch) Size() int {
	var size int
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		size += b.raftWriteBatchs[i].Size()
	}
	return size
}

func (b *WriteBatch) NumEntries() int {
	var numEntries int
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		numEntries += b.raftWriteBatchs[i].NumEntries()
	}
	return numEntries
}

func (b *WriteBatch) Reset() {
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		b.raftWriteBatchs[i].Reset()
	}
}

func (b *WriteBatch) IsEmpty() bool {
	var isEmpty bool = true
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		isEmpty = isEmpty && b.raftWriteBatchs[i].IsEmpty()
		if !isEmpty {
			return isEmpty
		}
	}
	return isEmpty
}

type Engine struct {
	index       int
	raftEngines []*engine
}

func Open(dir string, walSize int64, count int) (*Engine, error) {
	es := &Engine{
		raftEngines: make([]*engine, count),
	}
	for i := 0; i < count; i++ {
		var err error
		es.raftEngines[i], err = open(fmt.Sprintf("%s-%d", dir, i), walSize)
		if err != nil {
			return nil, err
		}
	}
	return es, nil
}

func (e *Engine) getEngine(regionID uint64) *engine {
	idx := hashRegionID(regionID) % uint64(len(e.raftEngines))
	return e.raftEngines[idx]
}

func (e *Engine) Write(wb *WriteBatch) error {
	if len(e.raftEngines) != len(wb.raftWriteBatchs) {
		return e.getEngine(wb.index).Write(wb.raftWriteBatchs[0])
	}
	for i := 0; i < len(wb.raftWriteBatchs); i++ {
		if uint64(i) != wb.index && !wb.raftWriteBatchs[i].IsEmpty() {
			err := e.raftEngines[i].Write(wb.raftWriteBatchs[i])
			if err != nil {
				return err
			}
		}
	}
	if !wb.raftWriteBatchs[wb.index].IsEmpty() {
		err := e.raftEngines[wb.index].Write(wb.raftWriteBatchs[wb.index])
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) IsEmpty() bool {
	var isEmpty bool = true
	for i := 0; i < len(e.raftEngines); i++ {
		isEmpty = isEmpty && e.raftEngines[i].IsEmpty()
		if !isEmpty {
			return isEmpty
		}
	}
	return isEmpty
}

func (e *Engine) GetState(regionID uint64, key []byte) []byte {
	return e.getEngine(regionID).GetState(regionID, key)
}

func (e *Engine) GetRegionRaftLogs(regionID uint64) *RegionRaftLogs {
	return e.getEngine(regionID).GetRegionRaftLogs(regionID)
}

func (e *Engine) IterateRegionStates(regionID uint64, desc bool, fn func(key, val []byte) error) error {
	return e.getEngine(regionID).IterateRegionStates(regionID, desc, fn)
}

func (e *Engine) IterateAllStates(desc bool, fn func(regionID uint64, key, val []byte) error) error {
	for i := 0; i < len(e.raftEngines); i++ {
		err := e.raftEngines[i].IterateAllStates(desc, fn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Close() error {
	for i := 0; i < len(e.raftEngines); i++ {
		err := e.raftEngines[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func hashRegionID(regionID uint64) uint64 {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, regionID)
	return farm.Fingerprint64(b)
}
