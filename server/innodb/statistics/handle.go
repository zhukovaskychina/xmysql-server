// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/context"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ddl"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	"sync/atomic"
	"time"
)

type statsCache map[int64]*Table

// Handle can update stats info periodically.
type Handle struct {
	ctx context.Context
	// LastVersion is the latest update version before last lease. Exported for test.
	LastVersion uint64
	// PrevLastVersion is the latest update version before two lease. Exported for test.
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// We can read the version with lastTwoVersion if the diff between commit time and version is less than one lease.
	// PrevLastVersion will be assigned by LastVersion every time Update is called.
	PrevLastVersion uint64
	statsCache      atomic.Value
	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *ddl.Event
	// analyzeResultCh is a channel to notify an analyze index or column operation has ended.
	// We need this to avoid updating the stats simultaneously.
	analyzeResultCh chan *AnalyzeResult
	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap tableDeltaMap

	Lease time.Duration
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.statsCache.Store(statsCache{})
	h.LastVersion = 0
	h.PrevLastVersion = 0
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	for len(h.analyzeResultCh) > 0 {
		<-h.analyzeResultCh
	}
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap)}
	h.globalMap = make(tableDeltaMap)
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx context.Context, lease time.Duration) *Handle {
	handle := &Handle{
		ctx:             ctx,
		ddlEventCh:      make(chan *ddl.Event, 100),
		analyzeResultCh: make(chan *AnalyzeResult, 100),
		listHead:        &SessionStatsCollector{mapper: make(tableDeltaMap)},
		globalMap:       make(tableDeltaMap),
		Lease:           lease,
	}
	handle.statsCache.Store(statsCache{})
	return handle
}

// AnalyzeResultCh returns analyze result channel in handle.
func (h *Handle) AnalyzeResultCh() chan *AnalyzeResult {
	return h.analyzeResultCh
}

// Update reads stats meta from storebytes and updates the stats map.
func (h *Handle) Update(is schemas.InfoSchema) error {
	//sql := fmt.Sprintf("SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %d order by version", h.PrevLastVersion)
	//rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	//if err != nil {
	//	return errors.Trace(err)
	//}
	//h.PrevLastVersion = h.LastVersion
	//tables := make([]*Table, 0, len(rows))
	//deletedTableIDs := make([]int64, 0, len(rows))
	//for _, row := range rows {
	//	version, tableID, modifyCount, count := row.Data[0].GetUint64(), row.Data[1].GetInt64(), row.Data[2].GetInt64(), row.Data[3].GetInt64()
	//	h.LastVersion = version
	//	table, ok := is.TableByID(tableID)
	//	if !ok {
	//		log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
	//		deletedTableIDs = append(deletedTableIDs, tableID)
	//		continue
	//	}
	//	tableInfo := table.Meta()
	//	tbl, err := h.tableStatsFromStorage(tableInfo)
	//	// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
	//	if err != nil {
	//		log.Errorf("Error occurred when read table stats for table id %d. The error message is %s.", tableID, errors.ErrorStack(err))
	//		continue
	//	}
	//	if tbl == nil {
	//		deletedTableIDs = append(deletedTableIDs, tableID)
	//		continue
	//	}
	//	tbl.Version = version
	//	tbl.Count = count
	//	tbl.ModifyCount = modifyCount
	//	tables = append(tables, tbl)
	//}
	//h.UpdateTableStats(tables, deletedTableIDs)
	return nil
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblID int64) *Table {
	tbl, ok := h.statsCache.Load().(statsCache)[tblID]
	if !ok {
		return PseudoTable(tblID)
	}
	return tbl
}

func (h *Handle) copyFromOldCache() statsCache {
	newCache := statsCache{}
	oldCache := h.statsCache.Load().(statsCache)
	for k, v := range oldCache {
		newCache[k] = v
	}
	return newCache
}

// UpdateTableStats updates the statistics table cache using copy on write.
func (h *Handle) UpdateTableStats(tables []*Table, deletedIDs []int64) {
	newCache := h.copyFromOldCache()
	for _, tbl := range tables {
		id := tbl.TableID
		newCache[id] = tbl
	}
	for _, id := range deletedIDs {
		delete(newCache, id)
	}
	h.statsCache.Store(newCache)
}
