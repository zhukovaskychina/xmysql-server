@echo off
setlocal enabledelayedexpansion
set GO111MODULE=on

:: 获取 Git 信息
for /f %%i in ('git describe --abbrev=0 --tags') do set VERSION=%%i
for /f %%i in ('git rev-list --count HEAD') do set REVCNT=%%i
for /f %%i in ('git rev-list --count %VERSION%') do set DEVCNT=%%i

if not "%REVCNT%"=="%DEVCNT%" (
    set /a DEVINC=%REVCNT% - %DEVCNT%
    set VERSION=%VERSION%.dev!DEVINC!
)
echo VER: %VERSION%

for /f %%i in ('git rev-parse HEAD') do set GITCOMMIT=%%i
for /f %%i in ('powershell -Command "Get-Date -Format yyyy/MM/dd-HH:mm:ss"') do set BUILDTIME=%%i

set LDFLAGS=-X main.VERSION=%VERSION% -X main.BUILDTIME=%BUILDTIME% -X main.GITCOMMIT=%GITCOMMIT%

:: 可选：附加 LDFLAGS
if not "%EX_LDFLAGS%"=="" (
    set LDFLAGS=%LDFLAGS% %EX_LDFLAGS%
)

:: 构建函数
call :build windows amd64 win-amd64.exe
call :buildClient windows amd64 win-amd64-client.exe

goto :eof

:build
echo Building %1 %2 ...
set GOOS=%1
set GOARCH=%2
go build -o dist\xmysql-server-%3 -tags vfs -ldflags "%LDFLAGS%" -gcflags "all=-N -l" main.go
goto :eof

:buildClient
echo Building Client %1 %2 ...
set GOOS=%1
set GOARCH=%2
go build -o dist\xmysql-server-client-%3 -tags vfs -ldflags "%LDFLAGS%" -gcflags "all=-N -l" client\main.go
goto :eof
