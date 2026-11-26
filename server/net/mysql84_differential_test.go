package net

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	stdnet "net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type mysql84QuerySnapshot struct {
	Columns     []string
	Types       []string
	Rows        [][]*string
	ErrorNumber uint16
	SQLState    string
	Error       string `json:",omitempty"`
}

func snapshotMySQLQuery(ctx context.Context, db *sql.DB, query string, prepared bool) mysql84QuerySnapshot {
	var result mysql84QuerySnapshot
	var rows *sql.Rows
	var err error
	if prepared {
		var statement *sql.Stmt
		statement, err = db.PrepareContext(ctx, query)
		if err == nil {
			defer statement.Close()
			rows, err = statement.QueryContext(ctx)
		}
	} else {
		rows, err = db.QueryContext(ctx, query)
	}
	if err == nil {
		defer rows.Close()
		result.Columns, err = rows.Columns()
		if err == nil {
			var columns []*sql.ColumnType
			columns, err = rows.ColumnTypes()
			for _, column := range columns {
				result.Types = append(result.Types, column.DatabaseTypeName())
			}
		}
		for err == nil && rows.Next() {
			values := make([]interface{}, len(result.Columns))
			destinations := make([]interface{}, len(values))
			for index := range values {
				destinations[index] = &values[index]
			}
			err = rows.Scan(destinations...)
			if err != nil {
				break
			}
			row := make([]*string, len(values))
			for index, value := range values {
				if value == nil {
					continue
				}
				text := fmt.Sprint(value)
				if bytes, ok := value.([]byte); ok {
					text = string(bytes)
				}
				row[index] = &text
			}
			result.Rows = append(result.Rows, row)
		}
		if err == nil {
			err = rows.Err()
		}
	}
	if err != nil {
		result.Error = err.Error()
		var mysqlError *mysql.MySQLError
		if errors.As(err, &mysqlError) {
			result.ErrorNumber = mysqlError.Number
			result.SQLState = string(mysqlError.SQLState[:])
		}
	}
	return result
}

// Opt-in comparison against an explicitly supplied local MySQL 8.4 test
// instance. Only tables created with this run's unique prefix are cleaned up.
// Results and protocol types are compared separately; a type mismatch must
// not be hidden by a successful value comparison.
func runMySQL84ReferenceComparison(t *testing.T, actual *sql.DB) {
	t.Helper()
	address := os.Getenv("XMYSQL_TEST_MYSQL84_ADDR")
	if address == "" {
		t.Skip("set XMYSQL_TEST_MYSQL84_ADDR and test credentials for live MySQL 8.4 comparison")
	}
	host, _, err := stdnet.SplitHostPort(address)
	require.NoError(t, err)
	ip := stdnet.ParseIP(host)
	require.NotNil(t, ip)
	require.True(t, ip.IsLoopback(), "reference fixture must use an explicit loopback address")
	config := mysql.NewConfig()
	config.Net, config.Addr, config.DBName = "tcp", address, "wire_compat"
	config.User, config.Passwd = os.Getenv("XMYSQL_TEST_MYSQL84_USER"), os.Getenv("XMYSQL_TEST_MYSQL84_PASSWORD")
	require.NotEmpty(t, config.User)
	config.Timeout, config.ReadTimeout, config.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	connector, err := mysql.NewConnector(config)
	require.NoError(t, err)
	reference := sql.OpenDB(connector)
	reference.SetMaxOpenConns(1)
	defer reference.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var version string
	require.NoError(t, reference.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version))
	require.True(t, strings.HasPrefix(version, "8.4."), "reference must be MySQL 8.4, got %s", version)
	t.Logf("live reference version: %s", version)
	prefix := fmt.Sprintf("diff_%x_", time.Now().UnixNano())
	replace := strings.NewReplacer(
		"diff_german2_latin", prefix+"german2_latin",
		"diff_german2", prefix+"german2",
		"diff_swedish", prefix+"swedish",
		"diff_mixed", prefix+"mixed",
		"diff_lhs", prefix+"lhs",
		"diff_rhs", prefix+"rhs",
		"diff_empty", prefix+"empty",
		"diff_types", prefix+"types",
	)
	for _, db := range []*sql.DB{actual, reference} {
		for _, query := range []string{
			"CREATE TABLE diff_lhs (id INT PRIMARY KEY,k INT,label VARCHAR(20))",
			"CREATE TABLE diff_rhs (id INT PRIMARY KEY,k INT,label VARCHAR(20))",
			"CREATE TABLE diff_empty (id INT PRIMARY KEY,k INT)",
			"CREATE TABLE diff_types (id INT PRIMARY KEY,flag TINYINT)",
			"CREATE TABLE diff_swedish (id INT PRIMARY KEY,s VARCHAR(20) COLLATE utf8mb4_swedish_ci)",
			"CREATE TABLE diff_german2 (id INT PRIMARY KEY,s VARCHAR(20) COLLATE utf8mb4_german2_ci)",
			"CREATE TABLE diff_german2_latin (id INT PRIMARY KEY,s VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_german2_ci)",
			"CREATE TABLE diff_mixed (id INT PRIMARY KEY,ci_text VARCHAR(20) COLLATE utf8mb4_general_ci,sv_text VARCHAR(20) COLLATE utf8mb4_swedish_ci,bin_text VARCHAR(20) COLLATE utf8mb4_bin,latin_text VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci)",
			"INSERT INTO diff_lhs VALUES (1,10,'a'),(2,20,'b'),(3,NULL,'c')",
			"INSERT INTO diff_rhs VALUES (1,10,'x'),(4,NULL,'y')",
			"INSERT INTO diff_types VALUES (1,1)",
			"INSERT INTO diff_swedish VALUES (1,'ä'),(2,'z'),(3,'å'),(4,'ö'),(5,'a')",
			"INSERT INTO diff_german2 VALUES (1,'a'),(2,'ä'),(3,'ae'),(4,'æ'),(5,'o'),(6,'ö'),(7,'oe'),(8,'œ'),(9,'u'),(10,'ü'),(11,'ue'),(12,'ß'),(13,'ss')",
			"INSERT INTO diff_german2_latin VALUES (1,'Ä'),(2,'AE'),(3,'Æ'),(4,'Ö'),(5,'OE'),(6,'Œ'),(7,'ß'),(8,'ss')",
			"INSERT INTO diff_mixed VALUES (1,'x','x','X','x'),(2,'A','A','a','A')",
		} {
			_, err := db.ExecContext(ctx, replace.Replace(query))
			require.NoError(t, err)
			if strings.HasPrefix(query, "CREATE TABLE ") {
				table := strings.Fields(replace.Replace(query))[2]
				defer func() {
					_, cleanupErr := db.ExecContext(context.Background(), "DROP TABLE "+table)
					require.NoError(t, cleanupErr)
				}()
			}
		}
	}
	type comparison struct {
		Query             string
		Prepared          bool
		Actual, Reference mysql84QuerySnapshot
	}
	var report []comparison
	for _, prepared := range []bool{false, true} {
		for _, query := range []string{
			"SELECT id,k,label FROM diff_lhs ORDER BY id",
			"SELECT id,k,k <=> NULL AS missing FROM diff_lhs ORDER BY id",
			"SELECT id FROM diff_lhs WHERE k NOT IN (SELECT k FROM diff_empty) ORDER BY id",
			"SELECT id FROM diff_lhs WHERE k IN (SELECT k FROM diff_empty) ORDER BY id",
			"SELECT id FROM diff_lhs WHERE k NOT IN (SELECT k FROM diff_rhs) ORDER BY id",
			"SELECT id FROM diff_lhs WHERE k > ALL (SELECT k FROM diff_empty) ORDER BY id",
			"SELECT id FROM diff_lhs WHERE k = ANY (SELECT k FROM diff_rhs) ORDER BY id",
			"SELECT l.id AS left_id,r.id AS right_id FROM diff_lhs l LEFT JOIN diff_rhs r ON l.k=r.k ORDER BY l.id",
			"SELECT k,diff_lhs.id AS left_id,diff_rhs.id AS right_id FROM diff_lhs RIGHT JOIN diff_rhs USING(k) ORDER BY diff_rhs.id",
			"SELECT * FROM diff_lhs NATURAL JOIN diff_rhs ORDER BY id",
			"SELECT count(*) AS n,count(k) AS nonnull,sum(k) AS total,avg(k) AS average_value FROM diff_empty",
			"SELECT k,count(*) AS total FROM diff_lhs GROUP BY k ORDER BY k",
			"SELECT l.id FROM diff_lhs l WHERE EXISTS (SELECT 1 FROM diff_rhs r WHERE r.id=l.id) ORDER BY l.id",
			"SELECT l.id,(SELECT r.k FROM diff_rhs r WHERE r.id=l.id) AS matching FROM diff_lhs l ORDER BY l.id",
			"SELECT l.id,l.k IN (SELECT r.k FROM diff_rhs r WHERE r.id=l.id) AS in_match FROM diff_lhs l ORDER BY l.id",
			"SELECT l.id,EXISTS (SELECT 1 FROM diff_rhs r WHERE r.id=l.id) AS exists_match FROM diff_lhs l ORDER BY l.id",
			"SELECT l.id FROM diff_lhs l LEFT JOIN diff_rhs r ON r.id=l.id WHERE EXISTS (SELECT 1 FROM diff_rhs x WHERE x.k=l.k) ORDER BY l.id",
			"SELECT l.k,COUNT(*) AS total FROM diff_lhs l GROUP BY l.k HAVING EXISTS (SELECT 1 FROM diff_rhs r WHERE r.k=l.k) ORDER BY l.k",
			"SELECT id FROM diff_lhs UNION ALL SELECT id FROM diff_rhs ORDER BY id",
			"WITH a AS (SELECT id,k FROM diff_lhs WHERE id<3) SELECT id,k FROM a ORDER BY id",
			"SELECT q.id,q.k FROM (SELECT id,k FROM diff_lhs WHERE id<3) q ORDER BY q.id",
			"SELECT (SELECT k FROM diff_rhs) AS too_many",
			"SELECT TRUE AS yes,FALSE AS no,1=1 AS equal_value,NULL IS NULL AS null_value",
			"SELECT flag,flag=1 AS enabled FROM diff_types",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY l.id",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY same_value",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY 1",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY l.id LIMIT 1",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY l.id DESC",
			"SELECT l.k IS NULL AS missing FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY l.id",
			"SELECT l.k<10 AS below FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY l.id",
			"SELECT l.k=r.k AS same_value FROM diff_lhs l JOIN diff_rhs r ON l.id=r.id ORDER BY l.k",
			"SELECT s FROM diff_swedish ORDER BY s",
			"SELECT COUNT(DISTINCT s) AS distinct_count FROM diff_swedish",
			"SELECT s FROM diff_german2 ORDER BY s,id",
			"SELECT COUNT(DISTINCT s) AS distinct_count FROM diff_german2",
			"SELECT s FROM diff_german2_latin ORDER BY s,id",
			"SELECT COUNT(DISTINCT s) AS distinct_count FROM diff_german2_latin",
			"SELECT id FROM diff_mixed WHERE ci_text=sv_text ORDER BY id",
			"SELECT ci_text COLLATE utf8mb4_bin=sv_text COLLATE utf8mb4_general_ci AS same FROM diff_mixed",
			"SELECT id FROM diff_mixed WHERE ci_text COLLATE utf8mb4_bin='A' ORDER BY id",
			"SELECT id FROM diff_mixed WHERE ci_text=bin_text ORDER BY id",
			"SELECT id FROM diff_mixed WHERE latin_text=bin_text ORDER BY id",
			"SELECT l.id FROM diff_mixed l JOIN diff_mixed r ON l.ci_text=r.sv_text ORDER BY l.id",
			"WITH a AS (SELECT id,k FROM diff_lhs) SELECT id,k FROM a WHERE id<0",
			"SELECT q.id,q.k FROM (SELECT id,k FROM diff_lhs) q WHERE q.id<0",
			"SELECT l.id,(SELECT r.k FROM diff_rhs r WHERE r.id=l.id) AS matching FROM diff_lhs l WHERE l.id<0",
			"SELECT l.id,(SELECT r.k FROM diff_rhs r WHERE r.id=l.id) AS matching FROM diff_lhs l ORDER BY l.id DESC",
			"SELECT k=10 AS eq_value,k<>10 AS ne_value,k<=>10 AS nullsafe_value,k<=>NULL AS nulltest_value,k<10 AS lt_value,k>=10 AS ge_value,k IS NULL AS isnull_value,k IS NOT NULL AS notnull_value,k BETWEEN 0 AND 20 AS range_value,k IN(10,20) AS in_value,k NOT IN(10,20) AS notin_value,(k=10 AND id=1) AS and_value,NOT(k=10) AS not_value,(k=10 OR id=3) AS or_value FROM diff_lhs ORDER BY id",
		} {
			t.Run(fmt.Sprintf("prepared_%v/%s", prepared, query), func(t *testing.T) {
				query = replace.Replace(query)
				observed := snapshotMySQLQuery(ctx, actual, query, prepared)
				expected := snapshotMySQLQuery(ctx, reference, query, prepared)
				report = append(report, comparison{Query: query, Prepared: prepared, Actual: observed, Reference: expected})
				require.Equal(t, expected.ErrorNumber, observed.ErrorNumber, "error code: %s", observed.Error)
				require.Equal(t, expected.SQLState, observed.SQLState)
				if expected.Error != "" || observed.Error != "" {
					require.NotEmpty(t, expected.Error)
					require.NotEmpty(t, observed.Error)
					require.Equal(t, expected.Columns, observed.Columns, "error result metadata must match the reference protocol path")
					require.Equal(t, expected.Types, observed.Types, "error result types must match the reference protocol path")
					return
				}
				require.Equal(t, expected.Columns, observed.Columns)
				require.Equal(t, expected.Rows, observed.Rows, "row values, NULLs, multiplicity and ordering")
				require.Equal(t, expected.Types, observed.Types, "protocol type parity is independent of row-value parity")
			})
		}
	}
	if path := os.Getenv("XMYSQL_TEST_MYSQL84_REPORT"); path != "" {
		encoded, err := json.MarshalIndent(struct {
			Version string
			Cases   []comparison
		}{version, report}, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, encoded, 0600))
	}
}
