package com.xmysql.server.test;

/** Shared endpoint and credential configuration for JDBC compatibility tests. */
final class JdbcTestConfig {
    private static final String DEFAULT_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";

    private JdbcTestConfig() {}

    static String url() {
        return configured("xmysql.jdbc.url", "XMYSQL_JDBC_URL", DEFAULT_URL);
    }

    static String url(String database) {
        String base = url();
        int queryStart = base.indexOf('?');
        String query = queryStart >= 0 ? base.substring(queryStart) : "";
        String authority = queryStart >= 0 ? base.substring(0, queryStart) : base;
        int pathStart = authority.indexOf('/', authority.indexOf("://") + 3);
        if (pathStart >= 0) {
            authority = authority.substring(0, pathStart);
        }
        return authority + "/" + database + query;
    }

    static String user() {
        return configured("xmysql.jdbc.user", "XMYSQL_JDBC_USER", "root");
    }

    static String password() {
        return configured("xmysql.jdbc.password", "XMYSQL_JDBC_PASSWORD", "root@1234");
    }

    private static String configured(String property, String environment, String fallback) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank()) {
            value = System.getenv(environment);
        }
        return value == null || value.isBlank() ? fallback : value;
    }
}
