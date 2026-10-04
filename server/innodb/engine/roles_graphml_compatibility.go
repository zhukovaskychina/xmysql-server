package engine

import (
	"html"
	"sort"
	"strconv"
	"strings"
)

type rolesGraphMLEdge struct {
	source string
	target string
}

// rolesGraphMLForAccounts renders the in-memory role edges in a deterministic
// GraphML document. The graph is intentionally limited to authorization
// identities and direct role edges; effective privilege expansion remains the
// responsibility of the existing role checker.
func rolesGraphMLForAccounts(file persistedAccountFile) string {
	nodes := make(map[string]struct{})
	edges := make([]rolesGraphMLEdge, 0)
	seenEdges := make(map[string]struct{})
	for _, account := range file.Accounts {
		grantee := strings.TrimSpace(account.User + "@" + account.Host)
		if grantee == "@" {
			continue
		}
		nodes[grantee] = struct{}{}
		for _, rawRole := range account.Roles {
			roles := roleReferences(rawRole)
			if len(roles) == 0 && strings.Contains(rawRole, "@") {
				roles = []string{strings.TrimSpace(rawRole)}
			}
			if len(roles) == 0 {
				continue
			}
			role := strings.TrimSpace(roles[0])
			if role == "" {
				continue
			}
			nodes[role] = struct{}{}
			key := role + "\x00" + grantee
			if _, exists := seenEdges[key]; exists {
				continue
			}
			seenEdges[key] = struct{}{}
			edges = append(edges, rolesGraphMLEdge{source: role, target: grantee})
		}
	}
	nodeNames := make([]string, 0, len(nodes))
	for node := range nodes {
		nodeNames = append(nodeNames, node)
	}
	sort.Strings(nodeNames)
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].source != edges[j].source {
			return edges[i].source < edges[j].source
		}
		return edges[i].target < edges[j].target
	})

	nodeIDs := make(map[string]string, len(nodeNames))
	for index, node := range nodeNames {
		nodeIDs[node] = "n" + strconv.Itoa(index)
	}
	var final strings.Builder
	final.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?><graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"><graph id=\"roles\" edgedefault=\"directed\">")
	for _, node := range nodeNames {
		final.WriteString("<node id=\"")
		final.WriteString(nodeIDs[node])
		final.WriteString("\"><data key=\"name\">")
		final.WriteString(html.EscapeString(node))
		final.WriteString("</data></node>")
	}
	for index, edge := range edges {
		final.WriteString("<edge id=\"e")
		final.WriteString(strconv.Itoa(index))
		final.WriteString("\" source=\"")
		final.WriteString(nodeIDs[edge.source])
		final.WriteString("\" target=\"")
		final.WriteString(nodeIDs[edge.target])
		final.WriteString("\"/>")
	}
	final.WriteString("</graph></graphml>")
	return final.String()
}
