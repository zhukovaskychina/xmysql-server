@echo off
REM XMySQL Server 测试运行脚本 (Windows)

echo ==========================================
echo   XMySQL Server 测试套件
echo ==========================================
echo.

REM 检查Maven是否安装
where mvn >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ❌ 错误: Maven未安装，请先安装Maven
    pause
    exit /b 1
)

REM 检查Java版本
echo 📋 检查Java版本...
java -version
echo.

REM 进入jdbc_client目录
cd /d "%~dp0"

REM 显示菜单
echo 请选择要运行的测试：
echo 1. 运行所有测试
echo 2. 运行DDL操作测试
echo 3. 运行DML操作测试
echo 4. 运行SELECT查询测试
echo 5. 运行JOIN查询测试
echo 6. 运行事务测试
echo 7. 运行系统变量测试
echo 8. 运行数据类型测试
echo 9. 运行索引和约束测试
echo 10. 运行PreparedStatement测试
echo 11. 运行性能测试
echo 12. 生成测试报告
echo 0. 退出
echo.

set /p choice="请输入选项 (0-12): "

if "%choice%"=="1" (
    echo 🚀 运行所有测试...
    mvn test
) else if "%choice%"=="2" (
    echo 🚀 运行DDL操作测试...
    mvn test -Dtest=DDLOperationsTest
) else if "%choice%"=="3" (
    echo 🚀 运行DML操作测试...
    mvn test -Dtest=DMLOperationsTest
) else if "%choice%"=="4" (
    echo 🚀 运行SELECT查询测试...
    mvn test -Dtest=SelectQueryTest
) else if "%choice%"=="5" (
    echo 🚀 运行JOIN查询测试...
    mvn test -Dtest=JoinQueryTest
) else if "%choice%"=="6" (
    echo 🚀 运行事务测试...
    mvn test -Dtest=TransactionTest
) else if "%choice%"=="7" (
    echo 🚀 运行系统变量测试...
    mvn test -Dtest=SystemVariableTest
) else if "%choice%"=="8" (
    echo 🚀 运行数据类型测试...
    mvn test -Dtest=DataTypeTest
) else if "%choice%"=="9" (
    echo 🚀 运行索引和约束测试...
    mvn test -Dtest=IndexAndConstraintTest
) else if "%choice%"=="10" (
    echo 🚀 运行PreparedStatement测试...
    mvn test -Dtest=PreparedStatementTest
) else if "%choice%"=="11" (
    echo 🚀 运行性能测试...
    mvn test -Dtest=PerformanceTest
) else if "%choice%"=="12" (
    echo 📊 生成测试报告...
    mvn test
    mvn surefire-report:report
    echo.
    echo ✅ 测试报告已生成在: target\surefire-reports\
) else if "%choice%"=="0" (
    echo 👋 退出
    exit /b 0
) else (
    echo ❌ 无效选项
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   测试完成
echo ==========================================
pause

