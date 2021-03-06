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
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/model"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sessionctx/variable"
)

type tableDeltaMap map[int64]variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	m[id] = item
}

func (m tableDeltaMap) merge(handle *SessionStatsCollector) {
	handle.Lock()
	defer handle.Unlock()
	for id, item := range handle.mapper {
		m.update(id, item.Delta, item.Count)
	}
	handle.mapper = make(tableDeltaMap)
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper tableDeltaMap
	prev   *SessionStatsCollector
	next   *SessionStatsCollector
	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *SessionStatsCollector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// Update will updates the delta and count for one table id.
func (s *SessionStatsCollector) Update(id int64, delta int64, count int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.update(id, delta, count)
}

// tryToRemoveFromList will remove this collector from the list if it's deleted flag is set.
func (s *SessionStatsCollector) tryToRemoveFromList() {
	s.Lock()
	defer s.Unlock()
	if !s.deleted {
		return
	}
	next := s.next
	prev := s.prev
	prev.next = next
	if next != nil {
		next.prev = prev
	}
}

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper: make(tableDeltaMap),
		next:   h.listHead.next,
		prev:   h.listHead,
	}
	if h.listHead.next != nil {
		h.listHead.next.prev = newCollector
	}
	h.listHead.next = newCollector
	return newCollector
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map. Then we dumps every table that held in map to KV.
func (h *Handle) DumpStatsDeltaToKV() error {
	h.listHead.Lock()
	for collector := h.listHead.next; collector != nil; collector = collector.next {
		collector.tryToRemoveFromList()
		h.globalMap.merge(collector)
	}
	h.listHead.Unlock()
	for id, item := range h.globalMap {
		updated, err := h.dumpTableStatDeltaToKV(id, item)
		if err != nil {
			return errors.Trace(err)
		}
		if updated {
			delete(h.globalMap, id)
		}
	}
	return nil
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatDeltaToKV(id int64, delta variable.TableDelta) (bool, error) {
	return false, nil
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/xmysql-server/server/innodb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

func needAnalyzeTable(tbl *Table, limit time.Duration) bool {

	return true
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is schemas.InfoSchema) error {
	dbs := is.AllSchemaNames()
	for _, db := range dbs {
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			statsTbl := h.GetTableStats(tblInfo.ID)
			if statsTbl.Pseudo || statsTbl.Count == 0 {
				continue
			}
			tblName := "`" + db + "`.`" + tblInfo.Name.O + "`"
			if needAnalyzeTable(statsTbl, 20*h.Lease) {
				sql := fmt.Sprintf("analyze table %s", tblName)
				log.Infof("[stats] auto analyze table %s now", tblName)
				return errors.Trace(h.execAutoAnalyze(sql))
			}
			for _, idx := range tblInfo.Indices {
				if idx.State != model.StatePublic {
					continue
				}
				if _, ok := statsTbl.Indices[idx.ID]; !ok {
					sql := fmt.Sprintf("analyze table %s index `%s`", tblName, idx.Name.O)
					log.Infof("[stats] auto analyze index `%s` for table %s now", idx.Name.O, tblName)
					return errors.Trace(h.execAutoAnalyze(sql))
				}
			}
		}
	}
	return nil
}

func (h *Handle) execAutoAnalyze(sql string) error {
	//startTime := time.Now()
	//_, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	//autoAnalyzeHistgram.Observe(time.Since(startTime).Seconds())
	//if err != nil {
	//	autoAnalyzeCounter.WithLabelValues("failed").Inc()
	//} else {
	//	autoAnalyzeCounter.WithLabelValues("succ").Inc()
	//}
	//return errors.Trace(err)
	return nil
}
