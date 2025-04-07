package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CalciteQueryProcessor {
    private final String jdbcUrl;
    private final FrameworkConfig frameworkConfig;
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    public CalciteQueryProcessor(final String duckDBPath) throws SQLException {
        this.jdbcUrl = (duckDBPath == null) ? "" : "jdbc:duckdb:" + duckDBPath;
        this.frameworkConfig = createFrameworkConfig();
    }

    private FrameworkConfig createFrameworkConfig() {
        try {
            DataSource dataSource = JdbcSchema.dataSource(
                    this.jdbcUrl,
                    "org.duckdb.DuckDBDriver",
                    "",
                    "");
            Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:");
            CalciteConnection calciteConn = calciteConnection.unwrap(CalciteConnection.class);

            SchemaPlus rootSchema = calciteConn.getRootSchema();
            SchemaPlus duckDbSchema = rootSchema.add("duckdb",
                    JdbcSchema.create(rootSchema, "duckdb", dataSource, null, null));

            return Frameworks.newConfigBuilder()
                    .defaultSchema(duckDbSchema)
                    .parserConfig(createParserConfig())
                    .build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private SqlParser.Config createParserConfig() {
        return SqlParser.config()
                .withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED)
                .withUnquotedCasing(Casing.TO_LOWER);
    }

    public void processSql(final String sql) {
        logger.info("Received query {}", sql);
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        try {
            SqlNode sqlNode = planner.parse(sql);
            logger.debug("Query parsed");
            SqlNode validateSqlNode = planner.validate(sqlNode);
            logger.debug("Query validated");
            final SqlString sqlString = validateSqlNode.toSqlString(new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT));
            logger.info("ToSqlString {}", sqlString.getSql());
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        } catch (ValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
