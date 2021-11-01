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

const BootstrapEngineIndex = 0

type WriteBatchs struct {
	region          bool
	index           uint64
	raftWriteBatchs []*WriteBatch
}

func NewRegionWriteBatch(regionID uint64) *WriteBatchs {
	bs := &WriteBatchs{
		region:          true,
		index:           regionID,
		raftWriteBatchs: []*WriteBatch{NewWriteBatch()},
	}
	return bs
}

func NewWriteBatchs(index uint64, count int) *WriteBatchs {
	bs := &WriteBatchs{
		index:           index,
		raftWriteBatchs: make([]*WriteBatch, count),
	}
	for i := 0; i < count; i++ {
		bs.raftWriteBatchs[i] = NewWriteBatch()
	}
	return bs
}

func (b *WriteBatchs) getWriteBatch(regionID uint64) *WriteBatch {
	idx := hashRegionID(regionID) % uint64(len(b.raftWriteBatchs))
	return b.raftWriteBatchs[idx]
}

func (b *WriteBatchs) AppendRaftLog(regionID uint64, entry *eraftpb.Entry) {
	b.getWriteBatch(regionID).AppendRaftLog(regionID, entry)
}

func (b *WriteBatchs) TruncateRaftLog(regionID, index uint64) {
	b.getWriteBatch(regionID).TruncateRaftLog(regionID, index)
}

func (b *WriteBatchs) SetState(regionID uint64, key, val []byte) {
	b.getWriteBatch(regionID).SetState(regionID, key, val)
}

func (b *WriteBatchs) Size() int {
	var size int
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		size += b.raftWriteBatchs[i].Size()
	}
	return size
}

func (b *WriteBatchs) NumEntries() int {
	var numEntries int
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		numEntries += b.raftWriteBatchs[i].NumEntries()
	}
	return numEntries
}

func (b *WriteBatchs) Reset() {
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		b.raftWriteBatchs[i].Reset()
	}
}

func (b *WriteBatchs) IsEmpty() bool {
	var isEmpty bool = true
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		isEmpty = isEmpty && b.raftWriteBatchs[i].IsEmpty()
		if !isEmpty {
			return isEmpty
		}
	}
	return isEmpty
}

type Engines struct {
	raftEngines []*Engine
}

func OpenEnginesByPaths(dirs []string, walSize int64) (*Engines, error) {
	es := &Engines{
		raftEngines: make([]*Engine, len(dirs)),
	}
	for i := 0; i < len(dirs); i++ {
		var err error
		es.raftEngines[i], err = Open(dirs[i], walSize)
		if err != nil {
			return nil, err
		}
	}
	return es, nil
}

func OpenEngines(dir string, walSize int64, count int) (*Engines, error) {
	es := &Engines{
		raftEngines: make([]*Engine, count),
	}
	for i := 0; i < count; i++ {
		var err error
		es.raftEngines[i], err = Open(fmt.Sprintf("%s-%d", dir, i), walSize)
		if err != nil {
			return nil, err
		}
	}
	return es, nil
}

func (e *Engines) getEngine(regionID uint64) *Engine {
	idx := hashRegionID(regionID) % uint64(len(e.raftEngines))
	return e.raftEngines[idx]
}

func (e *Engines) Write(wb *WriteBatchs) error {
	if wb.region {
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

func (e *Engines) IsEmpty() bool {
	var isEmpty bool = true
	for i := 0; i < len(e.raftEngines); i++ {
		isEmpty = isEmpty && e.raftEngines[i].IsEmpty()
		if !isEmpty {
			return isEmpty
		}
	}
	return isEmpty
}

func (e *Engines) GetState(regionID uint64, key []byte) []byte {
	return e.getEngine(regionID).GetState(regionID, key)
}

func (e *Engines) GetRegionRaftLogs(regionID uint64) *RegionRaftLogs {
	return e.getEngine(regionID).GetRegionRaftLogs(regionID)
}

func (e *Engines) IterateRegionStates(regionID uint64, desc bool, fn func(key, val []byte) error) error {
	return e.getEngine(regionID).IterateRegionStates(regionID, desc, fn)
}

func (e *Engines) IterateAllStates(desc bool, fn func(regionID uint64, key, val []byte) error) error {
	for i := 0; i < len(e.raftEngines); i++ {
		err := e.raftEngines[i].IterateAllStates(desc, fn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engines) Close() error {
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
