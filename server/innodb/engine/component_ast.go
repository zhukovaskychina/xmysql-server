package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func (e *XMySQLExecutor) executeParsedComponentLifecycle(ctx *ExecutionContext, session server.MySQLServerSession, stmt *sqlparser.ComponentStatement) (bool, error) {
	if stmt == nil {
		return false, nil
	}
	names := stmt.URNs
	if len(names) == 0 && stmt.URN != "" {
		names = []string{stmt.URN}
	}
	if len(names) == 0 {
		return true, fmt.Errorf("component name is required")
	}
	for _, name := range names {
		if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(name)), "file://") {
			return true, fmt.Errorf("invalid component name %q", name)
		}
	}
	if err := e.prepareDDLImplicitCommit(session); err != nil {
		return true, err
	}
	file, err := e.loadPersistedComponents()
	if err != nil {
		return true, err
	}
	if file.Components == nil {
		file.Components = []persistedComponent{}
	}
	if strings.EqualFold(stmt.Kind, "install") {
		if err := e.checkTablePrivilege(ctx, "mysql", "component", "INSERT"); err != nil {
			return true, err
		}
		nextID, nextGroup := int64(1), int64(1)
		for _, component := range file.Components {
			if component.ComponentID >= nextID {
				nextID = component.ComponentID + 1
			}
			if component.ComponentGroupID >= nextGroup {
				nextGroup = component.ComponentGroupID + 1
			}
		}
		for _, name := range names {
			for _, component := range file.Components {
				if strings.EqualFold(component.ComponentURN, name) {
					return true, fmt.Errorf("component %q is already installed", name)
				}
			}
			file.Components = append(file.Components, persistedComponent{ComponentID: nextID, ComponentGroupID: nextGroup, ComponentURN: name})
			nextID++
		}
	} else {
		if err := e.checkTablePrivilege(ctx, "mysql", "component", "DELETE"); err != nil {
			return true, err
		}
		for _, name := range names {
			found := false
			remaining := file.Components[:0]
			for _, component := range file.Components {
				if strings.EqualFold(component.ComponentURN, name) {
					found = true
					continue
				}
				remaining = append(remaining, component)
			}
			if !found {
				return true, fmt.Errorf("component %q is not installed", name)
			}
			file.Components = remaining
		}
	}
	if err := e.savePersistedComponents(file); err != nil {
		return true, err
	}
	if ctx != nil && ctx.Results != nil {
		ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "component statement executed successfully"}
	}
	return true, nil
}
