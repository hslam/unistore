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
	"fmt"
	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/log"
)

type EngineIndex int

const (
	NumOfEngines = 2

	FollowerEngineIndex EngineIndex = 0
	LeaderEngineIndex   EngineIndex = 1

	BootstrapEngineIndex = FollowerEngineIndex
)

type MergeWriteBatch struct {
	engineIndex     EngineIndex
	raftWriteBatchs []*WriteBatch
}

func NewMergeWriteBatch(engineIndex EngineIndex) *MergeWriteBatch {
	bs := &MergeWriteBatch{
		engineIndex:     engineIndex,
		raftWriteBatchs: make([]*WriteBatch, NumOfEngines),
	}
	for i := 0; i < len(bs.raftWriteBatchs); i++ {
		bs.raftWriteBatchs[i] = NewWriteBatch()
	}
	return bs
}

func (b *MergeWriteBatch) AppendRaftLog(regionID uint64, entry *eraftpb.Entry) {
	b.raftWriteBatchs[b.engineIndex].AppendRaftLog(regionID, entry)
}

func (b *MergeWriteBatch) TruncateRaftLog(regionID, index uint64) {
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		b.raftWriteBatchs[i].TruncateRaftLog(regionID, index)
	}
}

func (b *MergeWriteBatch) SetState(regionID uint64, key, val []byte, version uint64) {
	if len(val) > 0 {
		b.raftWriteBatchs[b.engineIndex].SetState(regionID, key, val, version)
	} else {
		for i := 0; i < len(b.raftWriteBatchs); i++ {
			b.raftWriteBatchs[i].SetState(regionID, key, val, version)
		}
	}
}

func (b *MergeWriteBatch) Size() int {
	var size int
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		if !b.raftWriteBatchs[i].IsEmpty() {
			size += b.raftWriteBatchs[i].Size()
		}
	}
	return size
}

func (b *MergeWriteBatch) NumEntries() int {
	var numEntries int
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		if !b.raftWriteBatchs[i].IsEmpty() {
			numEntries += b.raftWriteBatchs[i].NumEntries()
		}
	}
	return numEntries
}

func (b *MergeWriteBatch) Reset() {
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		b.raftWriteBatchs[i].Reset()
	}
}

func (b *MergeWriteBatch) IsEmpty() bool {
	var isEmpty = true
	for i := 0; i < len(b.raftWriteBatchs); i++ {
		isEmpty = isEmpty && b.raftWriteBatchs[i].IsEmpty()
		if !isEmpty {
			return isEmpty
		}
	}
	return isEmpty
}

type MergeEngine struct {
	raftEngines []*Engine
}

func OpenMergeEngines(dir string, walSize int64) (*MergeEngine, error) {
	es := &MergeEngine{
		raftEngines: make([]*Engine, NumOfEngines),
	}
	for i := 0; i < len(es.raftEngines); i++ {
		var err error
		es.raftEngines[i], err = Open(fmt.Sprintf("%s-%d", dir, i), walSize)
		if err != nil {
			return nil, err
		}
	}
	return es, nil
}

func (e *MergeEngine) GetEngine(idx EngineIndex) *Engine {
	return e.raftEngines[idx]
}

func (e *MergeEngine) Write(wb *MergeWriteBatch) error {
	for i := 0; i < len(e.raftEngines); i++ {
		if !wb.raftWriteBatchs[i].IsEmpty() {
			err := e.raftEngines[i].Write(wb.raftWriteBatchs[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *MergeEngine) IsEmpty() bool {
	var isEmpty = true
	for i := 0; i < len(e.raftEngines); i++ {
		isEmpty = isEmpty && e.raftEngines[i].IsEmpty()
		if !isEmpty {
			return isEmpty
		}
	}
	return isEmpty
}

func (e *MergeEngine) GetState(regionID uint64, key []byte) []byte {
	value, version := e.raftEngines[0].GetState(regionID, key)
	for i := 1; i < len(e.raftEngines); i++ {
		val, ver := e.raftEngines[i].GetState(regionID, key)
		if len(val) > 0 && ver >= version {
			version = ver
			value = val
		}
	}
	return value
}

func (re *RegionRaftLogs) Contains(low, high uint64) bool {
	if re.startIndex <= low && re.endIndex >= high && int(re.endIndex-re.startIndex) == len(re.raftLogs) {
		return true
	}
	return false
}

func (e *MergeEngine) GetRegionRaftLogs(regionID uint64) *RegionRaftLogs {
	var list = make([]*RegionRaftLogs, len(e.raftEngines))
	var rrl = e.raftEngines[0].GetRegionRaftLogs(regionID)
	list[0] = rrl
	for i := 1; i < len(e.raftEngines); i++ {
		list[i] = e.raftEngines[i].GetRegionRaftLogs(regionID)
	}
	if len(list[1].raftLogs) > 0 && len(list[0].raftLogs) == 0 && len(list) == 2 {
		return list[1]
	} else if len(list[0].raftLogs) > 0 && len(list[1].raftLogs) == 0 && len(list) == 2 {
		return list[0]
	}
	for i := 1; i < len(list); i++ {
		regionRaftLogs := list[i]
		if len(regionRaftLogs.raftLogs) > 0 {
			if regionRaftLogs.startIndex < rrl.startIndex {
				rrl.startIndex = regionRaftLogs.startIndex
			}
			if regionRaftLogs.endIndex > rrl.endIndex {
				rrl.endIndex = regionRaftLogs.endIndex
			}
			for idx, entry := range list[i].raftLogs {
				if op, ok := rrl.raftLogs[idx]; ok {
					if entry.index > 0 && entry.term >= op.term {
						rrl.raftLogs[idx] = entry
					}
				} else {
					rrl.raftLogs[idx] = entry
				}
			}
			wb := NewWriteBatch()
			wb.TruncateRaftLog(regionID, rrl.endIndex)
			e.raftEngines[i].Write(wb)
		}
	}
	length := int(rrl.endIndex - rrl.startIndex)
	if length != len(rrl.raftLogs) {
		log.S().Panicf("raft logs are not consecutive. %d, %d", length, len(rrl.raftLogs))
	} else if length != len(rrl.raftLogIndex) {
		rrl.raftLogIndex = rrl.raftLogIndex[:0]
		for i := rrl.startIndex; i < rrl.endIndex; i++ {
			rrl.raftLogIndex = append(rrl.raftLogIndex, i)
		}
	}
	return rrl
}

func (e *MergeEngine) IterateRegionStates(regionID uint64, desc bool, fn func(key, val []byte) error) error {
	mergeStates := btree.New(8)
	for i := 0; i < len(e.raftEngines); i++ {
		e.raftEngines[i].IterateRegionStates(regionID, desc, func(item *stateItem) error {
			if i := mergeStates.Get(item); i != nil {
				si := i.(*stateItem)
				if len(item.val) > 0 && item.version >= si.version {
					mergeStates.ReplaceOrInsert(item)
				}
			} else {
				mergeStates.ReplaceOrInsert(item)
			}
			return nil
		})
	}
	var err error
	startItem := &stateItem{regionID: regionID}
	endItem := &stateItem{regionID: regionID + 1}
	iterator := func(i btree.Item) bool {
		item := i.(*stateItem)
		err = fn(item.key, item.val)
		return err == nil
	}
	if desc {
		mergeStates.DescendRange(endItem, startItem, iterator)
	} else {
		mergeStates.AscendRange(startItem, endItem, iterator)
	}
	return err
}

func (e *MergeEngine) IterateAllStates(desc bool, fn func(regionID uint64, key, val []byte) error) error {
	mergeStates := btree.New(8)
	for i := 0; i < len(e.raftEngines); i++ {
		e.raftEngines[i].IterateAllStates(desc, func(item *stateItem) error {
			if i := mergeStates.Get(item); i != nil {
				si := i.(*stateItem)
				if len(item.val) > 0 && item.version >= si.version {
					mergeStates.ReplaceOrInsert(item)
				}
			} else {
				mergeStates.ReplaceOrInsert(item)
			}
			return nil
		})
	}
	var err error
	iterator := func(i btree.Item) bool {
		item := i.(*stateItem)
		err = fn(item.regionID, item.key, item.val)
		return err == nil
	}
	if desc {
		mergeStates.Descend(iterator)
	} else {
		mergeStates.Ascend(iterator)
	}
	return err
}

func (e *MergeEngine) Close() error {
	for i := 0; i < len(e.raftEngines); i++ {
		err := e.raftEngines[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}
