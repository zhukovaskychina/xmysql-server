#!/usr/bin/env bash
set -euo pipefail

reference_dsn="${1:?reference mysql DSN is required}"
target_dsn="${2:?target mysql DSN is required}"
report_dir="${3:-reports/compatibility}"
mkdir -p "$report_dir"
report_path="$report_dir/mysql84-matrix-$(date -u +%Y%m%d-%H%M%S).json"

cases=(
  "compat-parent-child|CREATE TABLE compat_parent(id INT PRIMARY KEY, code VARCHAR(20) UNIQUE); CREATE TABLE compat_child(id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_parent FOREIGN KEY(parent_id) REFERENCES compat_parent(id));"
  "foreign-key-read|INSERT INTO compat_parent VALUES (1, 'p1'); INSERT INTO compat_child VALUES (1, 1); SELECT id, parent_id FROM compat_child ORDER BY id;"
  "full-tables|SHOW FULL TABLES;"
  "columns|SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, ORDINAL_POSITION;"
)

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
passed=true
printf '{"generated_at":"%s","cases":[' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$report_path"
first=true
for item in "${cases[@]}"; do
  name="${item%%|*}"; sql="${item#*|}"
  ref_file="$tmp_dir/ref"; target_file="$tmp_dir/target"
  mysql "$reference_dsn" --batch --raw --skip-column-names -e "$sql" > "$ref_file"
  mysql "$target_dsn" --batch --raw --skip-column-names -e "$sql" > "$target_file"
  if ! cmp -s "$ref_file" "$target_file"; then passed=false; fi
  $first || printf ',' >> "$report_path"; first=false
  printf '{"name":"%s","passed":%s}' "$name" "$(cmp -s "$ref_file" "$target_file" && echo true || echo false)" >> "$report_path"
done
printf '],"passed":%s}\n' "$passed" >> "$report_path"
$passed
