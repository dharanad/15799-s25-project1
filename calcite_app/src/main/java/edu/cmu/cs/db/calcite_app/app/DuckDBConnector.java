package edu.cmu.cs.db.calcite_app.app;

import java.sql.*;
import java.util.Properties;

public class DuckDBConnector {
    // FIXME: Make me singleton
    public static Connection getConnection(String dbPath) throws SQLException {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("DuckDB Driver not found");
        }
        // Properties Ref Link : https://duckdb.org/docs/stable/configuration/overview.html
        final Properties properties = new Properties();
        // Use in memory db if path is not provided
        final String jdbcUrl = (dbPath == null) ? "" : "jdbc:duckdb:" + dbPath;
        return DriverManager.getConnection(jdbcUrl, properties);
    }

    public static void testRun() {
        try {
            final Connection connection = getConnection("cmu");
            final Statement statement = connection.createStatement();
            try(ResultSet resultSet = statement.executeQuery("select * from orders limit 10")) {
                while (resultSet.next()) {
                    // Columns are 1 based indices
                    System.out.println("o_custkey " + resultSet.getInt(2));
                }
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
