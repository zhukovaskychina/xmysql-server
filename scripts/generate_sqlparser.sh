#!/usr/bin/env sh
set -eu

sql_parser_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd -P)
sql_parser_goyacc_version='v0.30.0'
sql_parser_goyacc_package="golang.org/x/tools/cmd/goyacc@${sql_parser_goyacc_version}"
sql_parser_go=${SQLPARSER_GO:-go}
cd "$sql_parser_root/server/innodb/sqlparser"
exec "$sql_parser_go" run "$sql_parser_goyacc_package" -o sql.go ./sql.y
