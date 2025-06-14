/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

// FormatImpossibleQuery creates an impossible plan in a TrackedBuffer.
// An impossible plan is a modified version of a plan where all selects have where clauses that are
// impossible for mysql to resolve. This is used in the vtgate and vttablet:
//
// - In the vtgate it's used for joins: if the first plan returns no result, then vtgate uses the impossible
// plan just to fetch field info from vttablet
// - In the vttablet, it's just an optimization: the field info is fetched once form MySQL, cached and reused
// for subsequent queries
func FormatImpossibleQuery(buf *TrackedBuffer, node SQLNode) {
	switch node := node.(type) {
	case *Select:
		buf.Myprintf("select %v from %v where 1 != 1", node.SelectExprs, node.From)
		if node.GroupBy != nil {
			node.GroupBy.Format(buf)
		}
	case *Union:
		buf.Myprintf("%v %s %v", node.Left, node.Type, node.Right)
	default:
		node.Format(buf)
	}
}
