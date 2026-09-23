package engine

import "testing"

func TestCreateTableLikePattern(t *testing.T) {
	match := createTableLikePattern.FindStringSubmatch("create table copied_users like source_users")
	if len(match) != 4 || match[2] != "copied_users" || match[3] != "source_users" {
		t.Fatalf("unexpected CREATE TABLE LIKE match: %#v", match)
	}
}
