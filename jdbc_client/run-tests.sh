#!/bin/bash

# XMySQL Server 测试运行脚本

echo "=========================================="
echo "  XMySQL Server 测试套件"
echo "=========================================="
echo ""

# 检查Maven是否安装
if ! command -v mvn &> /dev/null; then
    echo "❌ 错误: Maven未安装，请先安装Maven"
    exit 1
fi

# 检查Java版本
echo "📋 检查Java版本..."
java -version
echo ""

# 进入jdbc_client目录
cd "$(dirname "$0")"

# 显示菜单
echo "请选择要运行的测试："
echo "1. 运行所有测试"
echo "2. 运行DDL操作测试"
echo "3. 运行DML操作测试"
echo "4. 运行SELECT查询测试"
echo "5. 运行JOIN查询测试"
echo "6. 运行事务测试"
echo "7. 运行系统变量测试"
echo "8. 运行数据类型测试"
echo "9. 运行索引和约束测试"
echo "10. 运行PreparedStatement测试"
echo "11. 运行性能测试"
echo "12. 生成测试报告"
echo "0. 退出"
echo ""

read -p "请输入选项 (0-12): " choice

case $choice in
    1)
        echo "🚀 运行所有测试..."
        mvn test
        ;;
    2)
        echo "🚀 运行DDL操作测试..."
        mvn test -Dtest=DDLOperationsTest
        ;;
    3)
        echo "🚀 运行DML操作测试..."
        mvn test -Dtest=DMLOperationsTest
        ;;
    4)
        echo "🚀 运行SELECT查询测试..."
        mvn test -Dtest=SelectQueryTest
        ;;
    5)
        echo "🚀 运行JOIN查询测试..."
        mvn test -Dtest=JoinQueryTest
        ;;
    6)
        echo "🚀 运行事务测试..."
        mvn test -Dtest=TransactionTest
        ;;
    7)
        echo "🚀 运行系统变量测试..."
        mvn test -Dtest=SystemVariableTest
        ;;
    8)
        echo "🚀 运行数据类型测试..."
        mvn test -Dtest=DataTypeTest
        ;;
    9)
        echo "🚀 运行索引和约束测试..."
        mvn test -Dtest=IndexAndConstraintTest
        ;;
    10)
        echo "🚀 运行PreparedStatement测试..."
        mvn test -Dtest=PreparedStatementTest
        ;;
    11)
        echo "🚀 运行性能测试..."
        mvn test -Dtest=PerformanceTest
        ;;
    12)
        echo "📊 生成测试报告..."
        mvn test
        mvn surefire-report:report
        echo ""
        echo "✅ 测试报告已生成在: target/surefire-reports/"
        ;;
    0)
        echo "👋 退出"
        exit 0
        ;;
    *)
        echo "❌ 无效选项"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "  测试完成"
echo "=========================================="

