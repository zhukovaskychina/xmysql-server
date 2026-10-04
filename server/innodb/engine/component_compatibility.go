package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
)

type persistedComponent struct {
	ComponentID      int64  `json:"component_id"`
	ComponentGroupID int64  `json:"component_group_id"`
	ComponentURN     string `json:"component_urn"`
}

type persistedComponentFile struct {
	Components []persistedComponent `json:"components"`
}

func (e *XMySQLExecutor) componentFilePath() string {
	return filepath.Join(e.getDataDir(), "mysql", "components.json")
}

func (e *XMySQLExecutor) loadPersistedComponents() (persistedComponentFile, error) {
	raw, err := os.ReadFile(e.componentFilePath())
	if os.IsNotExist(err) {
		return persistedComponentFile{Components: []persistedComponent{}}, nil
	}
	if err != nil {
		return persistedComponentFile{}, err
	}
	if len(raw) == 0 {
		return persistedComponentFile{Components: []persistedComponent{}}, nil
	}
	var file persistedComponentFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return persistedComponentFile{}, err
	}
	return file, nil
}

func (e *XMySQLExecutor) savePersistedComponents(file persistedComponentFile) error {
	sort.Slice(file.Components, func(i, j int) bool {
		return file.Components[i].ComponentID < file.Components[j].ComponentID
	})
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return writeMetadataFileAtomic(e.componentFilePath(), raw)
}

var componentStatementPattern = regexp.MustCompile(`(?is)^\s*(install|uninstall)\s+component\s+(.+?)\s*;?\s*$`)
var componentURNPattern = regexp.MustCompile(`(?is)'((?:''|[^'])*)'|"((?:""|[^"])*)"`)

func parseComponentStatement(query string) (string, []string, error) {
	match := componentStatementPattern.FindStringSubmatch(query)
	if len(match) != 3 {
		return "", nil, nil
	}
	rest := strings.TrimSpace(match[2])
	if setIndex := regexp.MustCompile(`(?is)\s+set\s+`).FindStringIndex(rest); setIndex != nil {
		rest = strings.TrimSpace(rest[:setIndex[0]])
	}
	names := make([]string, 0)
	for _, quoted := range componentURNPattern.FindAllStringSubmatch(rest, -1) {
		name := quoted[1]
		if name == "" {
			name = strings.ReplaceAll(quoted[2], `""`, `"`)
		} else {
			name = strings.ReplaceAll(name, "''", "'")
		}
		name = strings.TrimSpace(name)
		if !strings.HasPrefix(strings.ToLower(name), "file://") {
			return "", nil, fmt.Errorf("invalid component name %q", name)
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return "", nil, fmt.Errorf("component name is required")
	}
	return strings.ToLower(match[1]), names, nil
}

// executeComponentLifecycle persists the component registry rows. xmysql does
// not load native component binaries, so this closes the SQL/registry lifecycle
// while deliberately leaving native component service activation out of scope.
func (e *XMySQLExecutor) executeComponentLifecycle(ctx *ExecutionContext, session server.MySQLServerSession, query string) (bool, error) {
	verb, names, err := parseComponentStatement(query)
	if verb == "" {
		return false, nil
	}
	if err != nil {
		return true, err
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
	if verb == "install" {
		if err := e.checkTablePrivilege(ctx, "mysql", "component", "INSERT"); err != nil {
			return true, err
		}
		var nextID, nextGroup int64 = 1, 1
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
	return true, nil
}
