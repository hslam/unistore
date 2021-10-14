// Copyright 2019-present PingCAP, Inc.
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

package raftstore

import (
	"github.com/ngaut/unistore/engine"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/raftengine"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
)

func BootstrapStore(engines *Engines, clussterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	if engines.kv.Size() > 1 {
		return errors.New("kv store is not empty and ahs alread had data.")
	}
	if !engines.raft.IsEmpty() {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clussterID
	ident.StoreId = storeID
	wb := raftengine.NewMergeWriteBatch(raftengine.BootstrapEngineIndex)
	val, err := ident.Marshal()
	if err != nil {
		return err
	}
	wb.SetState(0, StoreIdentKey(), val, 0)
	return engines.raft.Write(wb)
}

var (
	rawInitialStartKey = []byte{2}
	rawInitialEndKey   = []byte{255, 255, 255, 255, 255, 255, 255, 255}
)

func PrepareBootstrap(engines *Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := newBootstrapRegion(regionID, peerID, storeID)
	err := writePrepareBootstrap(engines, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func writePrepareBootstrap(engines *Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	raftWB := raftengine.NewMergeWriteBatch(raftengine.BootstrapEngineIndex)
	val, _ := state.Marshal()
	raftWB.SetState(0, PrepareBootstrapKey(), val, 0)
	raftWB.SetState(region.Id, RegionStateKey(region.RegionEpoch.Version, region.RegionEpoch.ConfVer), val, region.RegionEpoch.Version)
	writeInitialRaftState(raftWB, region)
	ingestTree := initialIngestTree(region.Id, region.RegionEpoch.Version)
	csBin, _ := ingestTree.ChangeSet.Marshal()
	raftWB.SetState(region.Id, KVEngineMetaKey(), csBin, ingestTree.ChangeSet.ShardVer)
	err := engines.raft.Write(raftWB)
	if err != nil {
		return err
	}
	return engines.kv.Ingest(ingestTree)
}

func initialIngestTree(regionID, version uint64) *engine.IngestTree {
	return &engine.IngestTree{
		ChangeSet: &enginepb.ChangeSet{
			ShardID:  regionID,
			ShardVer: version,
			Sequence: RaftInitLogIndex,
			Snapshot: &enginepb.Snapshot{
				Start: nil,
				End:   engine.GlobalShardEndKey,
				Properties: &enginepb.Properties{
					ShardID: regionID,
					Keys:    []string{applyStateKey},
					Values:  [][]byte{newInitialApplyState().Marshal()},
				},
			},
		},
	}
}

func newBootstrapRegion(regionID, peerID, storeID uint64) *metapb.Region {
	return &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: []*metapb.Peer{
			{
				Id:      peerID,
				StoreId: storeID,
			},
		},
	}
}

func writeInitialRaftState(raftWB *raftengine.MergeWriteBatch, region *metapb.Region) {
	raftState := raftState{
		lastIndex: RaftInitLogIndex,
		term:      RaftInitLogTerm,
		commit:    RaftInitLogIndex,
	}
	raftWB.SetState(region.Id, RaftStateKey(region.RegionEpoch.Version), raftState.Marshal(), region.RegionEpoch.Version)
}

func ClearPrepareBootstrap(engines *Engines, region *metapb.Region) error {
	for i := 0; i < raftengine.NumOfEngines; i++ {
		wb := raftengine.NewMergeWriteBatch(raftengine.EngineIndex(i))
		if raftengine.EngineIndex(i) == raftengine.BootstrapEngineIndex {
			wb.SetState(0, PrepareBootstrapKey(), nil, 0)
		}
		wb.SetState(region.Id, RegionStateKey(region.RegionEpoch.Version, region.RegionEpoch.ConfVer), nil, 0)
		wb.SetState(region.Id, RaftStateKey(region.RegionEpoch.Version), nil, 0)
		wb.SetState(region.Id, KVEngineMetaKey(), nil, 0)
		err := engines.raft.Write(wb)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return engines.kv.RemoveShard(region.Id, true)
}

func ClearPrepareBootstrapState(engines *Engines) error {
	wb := raftengine.NewMergeWriteBatch(raftengine.BootstrapEngineIndex)
	wb.SetState(0, PrepareBootstrapKey(), nil, 0)
	err := engines.raft.Write(wb)
	return errors.WithStack(err)
}
