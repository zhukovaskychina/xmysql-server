package com.xmysql.server.test;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * 测试套件 - 运行所有测试
 * 
 * 使用方式：
 * mvn test -Dtest=AllTestsSuite
 */
@Suite
@SuiteDisplayName("XMySQL Server 完整测试套件")
@SelectClasses({
    DDLOperationsTest.class,
    DMLOperationsTest.class,
    SelectQueryTest.class,
    JoinQueryTest.class,
    TransactionTest.class,
    SystemVariableTest.class,
    DataTypeTest.class,
    IndexAndConstraintTest.class,
    PreparedStatementTest.class,
    PerformanceTest.class
})
public class AllTestsSuite {
    // 测试套件类，不需要实现任何方法
    // JUnit 5会自动运行所有指定的测试类
}

