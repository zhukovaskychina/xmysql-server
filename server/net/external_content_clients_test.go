package net

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runExternalContentClients(t *testing.T, endpoint string) {
	t.Helper()
	if os.Getenv("XMYSQL_RUN_EXTERNAL_CONTENT_CLIENTS") != "1" {
		t.Skip("set XMYSQL_RUN_EXTERNAL_CONTENT_CLIENTS=1 and test driver paths to exercise Python/JDBC")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "client_compatibility"))
	require.NoError(t, err)
	python := os.Getenv("XMYSQL_TEST_PYTHON")
	java := os.Getenv("XMYSQL_TEST_JAVA")
	jar := os.Getenv("XMYSQL_TEST_JDBC_JAR")
	require.NotEmpty(t, python, "XMYSQL_TEST_PYTHON must name a Python executable with PyMySQL installed")
	require.NotEmpty(t, java, "XMYSQL_TEST_JAVA must name a Java 17+ executable")
	require.FileExists(t, jar, "XMYSQL_TEST_JDBC_JAR must name Connector/J")
	for _, client := range []struct {
		name       string
		executable string
		args       []string
	}{
		{"python", python, []string{filepath.Join(root, "python", "content_runner.py")}},
		{"java", java, []string{"-cp", jar, filepath.Join(root, "java", "ContentRunner.java")}},
	} {
		t.Run(client.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			command := exec.CommandContext(ctx, client.executable, client.args...)
			command.Env = append(os.Environ(), "XMYSQL_CLIENT_DSN="+endpoint, "XMYSQL_CLIENT_USER=query_wire", "XMYSQL_CLIENT_PASSWORD=query-test-only")
			output, err := command.CombinedOutput()
			require.NoError(t, err, "external client output: %s", output)
			var result struct {
				Status string `json:"status"`
			}
			require.NoError(t, json.Unmarshal(output, &result), "client must emit an acceptance result: %s", output)
			require.Equal(t, "PASS", result.Status)
			t.Logf("external client: %s", output)
		})
	}
}
