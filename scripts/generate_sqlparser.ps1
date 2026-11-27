$ErrorActionPreference = 'Stop'

$sqlParserRoot = Split-Path -Parent $PSScriptRoot
$sqlParserGoyaccVersion = 'v0.30.0'
$sqlParserGoyaccPackage = "golang.org/x/tools/cmd/goyacc@$sqlParserGoyaccVersion"
$sqlParserGo = if ($env:SQLPARSER_GO) { $env:SQLPARSER_GO } else { (Get-Command go -ErrorAction Stop).Source }
Push-Location (Join-Path $sqlParserRoot 'server/innodb/sqlparser')
try {
    & $sqlParserGo run $sqlParserGoyaccPackage -o sql.go ./sql.y
    if ($LASTEXITCODE -ne 0) { throw "goyacc failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}
