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

package plan

import (
	"bytes"
	"encoding/json"
	"fmt"
	types "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	"strings"

	"github.com/juju/errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/expression"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/model"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/util/auth"

	"github.com/zhukovaskychina/xmysql-server/server/mysql"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	basePlan
}

// ShowDDLJobs is for showing DDL job list.
type ShowDDLJobs struct {
	basePlan
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	basePlan

	Tables []*ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	basePlan

	JobIDs []int64
}

// SelectLock represents a select lock plan.
type SelectLock struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

	Lock ast.SelectLockType
}

// Prepare represents prepare plan.
type Prepare struct {
	basePlan

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	basePlan

	Name      string
	UsingVars []expression.Expression
	ExecID    uint32
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	basePlan

	Name string
}

// Show represents a show plan.
type Show struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

	Tp     ast.ShowStmtType // Databases/Tables/Columns/....
	DBName string
	Table  *ast.TableName  // Used for showing columns.
	Column *ast.ColumnName // Used for `desc table column`.
	Flag   int             // Some flag parsed from sql, such as FULL.
	Full   bool
	User   *auth.UserIdentity // Used for show grants.

	// Used by show variables
	GlobalScope bool
}

// Set represents a plan for set stmt.
type Set struct {
	basePlan

	VarAssigns []*expression.VarAssignment
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	basePlan

	Statement ast.StmtNode
}

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Columns      []*ast.ColumnName
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

	Table       schemas.Table
	tableSchema *expression.Schema
	Columns     []*ast.ColumnName
	Lists       [][]expression.Expression
	Setlist     []*expression.Assignment
	OnDuplicate []*expression.Assignment

	IsReplace bool
	Priority  mysql.PriorityEnum
	IgnoreErr bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	GenCols InsertGeneratedColumns
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	TableInfo *model.TableInfo
	PKInfo    *model.ColumnInfo
	ColsInfo  []*model.ColumnInfo
	PushDown  bool
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	TableInfo *model.TableInfo
	IndexInfo *model.IndexInfo
	PushDown  bool
}

// Analyze represents an analyze plan
type Analyze struct {
	basePlan

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
}

// LoadData represents a loaddata plan.
type LoadData struct {
	basePlan

	IsLocal    bool
	Path       string
	Table      *ast.TableName
	Columns    []*ast.ColumnName
	FieldsInfo *ast.FieldsClause
	LinesInfo  *ast.LinesClause

	GenCols InsertGeneratedColumns
}

// DDL represents a DDL statement plan.
type DDL struct {
	basePlan

	Statement ast.DDLNode
}

// Explain represents a explain plan.
type Explain struct {
	basePlan

	StmtPlan       Plan
	Rows           [][]types.Datum
	explainedPlans map[int]bool
}

func (e *Explain) prepareExplainInfo(p Plan, parent Plan) error {
	for _, child := range p.Children() {
		err := e.prepareExplainInfo(child, p)
		if err != nil {
			return errors.Trace(err)
		}
	}
	explain, err := json.MarshalIndent(p, "", "    ")
	if err != nil {
		return errors.Trace(err)
	}
	parentStr := ""
	if parent != nil {
		parentStr = parent.ExplainID()
	}
	row := types.MakeDatums(p.ExplainID(), string(explain), parentStr)
	e.Rows = append(e.Rows, row)
	return nil
}

// prepareExplainInfo4DAGTask generates the following information for every plan:
// ["id", "parents", "task", "operator info"].
func (e *Explain) prepareExplainInfo4DAGTask(p PhysicalPlan, taskType string) {
	parents := p.Parents()
	parentIDs := make([]string, 0, len(parents))
	for _, parent := range parents {
		parentIDs = append(parentIDs, parent.ExplainID())
	}
	childrenIDs := make([]string, 0, len(p.Children()))
	for _, ch := range p.Children() {
		childrenIDs = append(childrenIDs, ch.ExplainID())
	}
	parentInfo := strings.Join(parentIDs, ",")
	childrenInfo := strings.Join(childrenIDs, ",")
	operatorInfo := p.ExplainInfo()
	count := p.statsProfile().count
	row := types.MakeDatums(p.ExplainID(), parentInfo, childrenInfo, taskType, operatorInfo, count)
	e.Rows = append(e.Rows, row)
}

// prepareCopTaskInfo generates explain information for cop-tasks.
// Only PhysicalTableReader, PhysicalIndexReader and PhysicalIndexLookUpReader have cop-tasks currently.
func (e *Explain) prepareCopTaskInfo(plans []PhysicalPlan) {
	for _, p := range plans {
		e.prepareExplainInfo4DAGTask(p, "cop")
	}
}

// prepareRootTaskInfo generates explain information for root-tasks.
func (e *Explain) prepareRootTaskInfo(p PhysicalPlan) {
	e.explainedPlans[p.ID()] = true
	for _, child := range p.Children() {
		if e.explainedPlans[child.ID()] {
			continue
		}
		e.prepareRootTaskInfo(child.(PhysicalPlan))
	}
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		e.prepareCopTaskInfo(copPlan.TablePlans)
	case *PhysicalIndexReader:
		e.prepareCopTaskInfo(copPlan.IndexPlans)
	case *PhysicalIndexLookUpReader:
		e.prepareCopTaskInfo(copPlan.IndexPlans)
		e.prepareCopTaskInfo(copPlan.TablePlans)
	}
	e.prepareExplainInfo4DAGTask(p, "root")
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	buffer.WriteString(fmt.Sprintf("\ndigraph %s {\n", p.ExplainID()))
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString(fmt.Sprintln("}"))

	row := types.MakeDatums(buffer.String())
	e.Rows = append(e.Rows, row)
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("subgraph cluster%v{\n", p.ID()))
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	buffer.WriteString(fmt.Sprintf("label = \"%s\"\n", taskTp))

	if len(p.Children()) == 0 {
		buffer.WriteString(fmt.Sprintf("\"%s\"\n}\n", p.ExplainID()))
		return
	}

	copTasks := []Plan{}
	pipelines := []string{}

	for planQueue := []Plan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
			copTasks = append(copTasks, copPlan.indexPlan)
		}
		for _, child := range curPlan.Children() {
			buffer.WriteString(fmt.Sprintf("\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID()))
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop.(PhysicalPlan), "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}
