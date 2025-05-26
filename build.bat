@echo off
setlocal enabledelayedexpansion
set GO111MODULE=on

echo ========================================
echo Building xmysql-server and client
echo ========================================

:: 创建 dist 目录结构
if not exist "dist" mkdir dist
if not exist "dist\conf" mkdir dist\conf

:: 获取 Git 信息
for /f %%i in ('git describe --abbrev=0 --tags 2^>nul') do set VERSION=%%i
if "%VERSION%"=="" set VERSION=v1.0.0

for /f %%i in ('git rev-list --count HEAD 2^>nul') do set REVCNT=%%i
if "%REVCNT%"=="" set REVCNT=0

for /f %%i in ('git rev-list --count %VERSION% 2^>nul') do set DEVCNT=%%i
if "%DEVCNT%"=="" set DEVCNT=0

if not "%REVCNT%"=="%DEVCNT%" (
    set /a DEVINC=%REVCNT% - %DEVCNT%
    set VERSION=%VERSION%.dev!DEVINC!
)
echo Version: %VERSION%

for /f %%i in ('git rev-parse HEAD 2^>nul') do set GITCOMMIT=%%i
if "%GITCOMMIT%"=="" set GITCOMMIT=unknown

for /f %%i in ('powershell -Command "Get-Date -Format yyyy/MM/dd-HH:mm:ss"') do set BUILDTIME=%%i

set LDFLAGS=-X main.VERSION=%VERSION% -X main.BUILDTIME=%BUILDTIME% -X main.GITCOMMIT=%GITCOMMIT%

:: 可选：附加 LDFLAGS
if not "%EX_LDFLAGS%"=="" (
    set LDFLAGS=%LDFLAGS% %EX_LDFLAGS%
)

echo ========================================
echo Building xmysql-server...
echo ========================================
call :buildServer windows amd64 xmysql-server.exe

echo ========================================
echo Building xmysql-client...
echo ========================================
call :buildClient windows amd64 xmysql-client.exe

echo ========================================
echo Copying configuration files...
echo ========================================
call :copyConfigs

echo ========================================
echo Build completed successfully!
echo ========================================
echo Output directory: dist\
echo - xmysql-server.exe
echo - xmysql-client.exe
echo - conf\my.ini
echo - conf\default.ini
echo - conf\log.xml
echo - conf\config.yml
echo ========================================

goto :eof

:buildServer
echo Building xmysql-server for %1 %2...
set GOOS=%1
set GOARCH=%2
go build -o dist\%3 -tags vfs -ldflags "%LDFLAGS%" -gcflags "all=-N -l" main.go
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to build xmysql-server
    exit /b 1
)
echo Successfully built: dist\%3
goto :eof

:buildClient
echo Building xmysql-client for %1 %2...
set GOOS=%1
set GOARCH=%2
go build -o dist\%3 -tags vfs -ldflags "%LDFLAGS%" -gcflags "all=-N -l" client\main.go
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to build xmysql-client
    exit /b 1
)
echo Successfully built: dist\%3
goto :eof

:copyConfigs
echo Copying configuration files from conf\ to dist\conf\...
copy conf\my.ini dist\conf\ >nul
if %ERRORLEVEL% neq 0 (
    echo Warning: Failed to copy my.ini
) else (
    echo Copied: my.ini
)

copy conf\default.ini dist\conf\ >nul
if %ERRORLEVEL% neq 0 (
    echo Warning: Failed to copy default.ini
) else (
    echo Copied: default.ini
)

copy conf\log.xml dist\conf\ >nul
if %ERRORLEVEL% neq 0 (
    echo Warning: Failed to copy log.xml
) else (
    echo Copied: log.xml
)

copy conf\config.yml dist\conf\ >nul
if %ERRORLEVEL% neq 0 (
    echo Warning: Failed to copy config.yml
) else (
    echo Copied: config.yml
)
goto :eof
