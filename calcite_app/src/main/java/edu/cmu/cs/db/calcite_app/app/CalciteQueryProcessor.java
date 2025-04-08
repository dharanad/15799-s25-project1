package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

public class CalciteQueryProcessor {
    private final String jdbcUrl;
    private final FrameworkConfig frameworkConfig;
    private Prepare.CatalogReader catalogReader;
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    public CalciteQueryProcessor(final String duckDBPath) {
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
                    .ruleSets(Collections.emptyList()) // Ref: https://calcite.apache.org/javadocAggregate/org/apache/calcite/plan/RelOptRule.html
                    .programs(Collections.emptyList()) // Ref: https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/Program.html
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

    private String sqlNodeToString(final SqlNode node) {
        return node.toSqlString(new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT)).getSql();
    }

//    public void processSql(final String sql) {
//        logger.info("Received query : {}", sql);
//        Planner planner = Frameworks.getPlanner(frameworkConfig);
//        try {
//            SqlNode sqlNode = planner.parse(sql);
//            SqlNode validateSqlNode = planner.validate(sqlNode);
//            logger.debug("ToSqlString {}", sqlNodeToString(validateSqlNode));
//            RelRoot rootRel = planner.rel(sqlNode);
//            final String plan = explainPlan(rootRel.rel);
//            logger.info("Before {}", plan);
//            logger.info("After {}", explainPlan(optimize(rootRel.rel)));
//        } catch (SqlParseException e) {
//            logger.error("Error parsing query", e);
//            throw new RuntimeException(e);
//        } catch (ValidationException e) {
//            logger.error("Error validating query", e);
//            throw new RuntimeException(e);
//        } catch (RelConversionException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void processSql2(final String sql) {
        final SqlParser sqlParser = SqlParser.create(sql, frameworkConfig.getParserConfig());
        try {
            final SqlNode sqlNode = sqlParser.parseStmt();
            final SqlValidator validator = createValidatorFromJdbc();
            final SqlNode validateNode = validator.validate(sqlNode);
            final RelRoot root = sqlToRel(validateNode, validator, this.catalogReader);
            logger.info("After {}", explainPlan(root.rel));
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private SqlValidator createValidatorFromJdbc() throws SQLException {
        final DataSource dataSource = JdbcSchema.dataSource(
                this.jdbcUrl,
                "org.duckdb.DuckDBDriver",
                "",
                "");

        Properties props = new Properties();
        final CalciteConnection calciteConnection = DriverManager.getConnection("jdbc:calcite:", props)
                .unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SchemaPlus duckDbSchema = rootSchema.add("duckdb",
                JdbcSchema.create(rootSchema, "duckdb", dataSource, null, null));

        RelDataTypeFactory typeFactory = calciteConnection.getTypeFactory();

        this.catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(duckDbSchema),
                CalciteSchema.from(duckDbSchema).path(null),
                typeFactory,
                calciteConnection.config());

        return SqlValidatorUtil.newValidator(
                frameworkConfig.getOperatorTable(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT);
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new HepPlanner(HepProgram.builder().build());
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private RelRoot sqlToRel(SqlNode sqlNode, SqlValidator validator, Prepare.CatalogReader catalogReader) throws SQLException {
        SqlToRelConverter converter = new SqlToRelConverter(
                (rowType, queryString, schemaPath, viewPath) -> null,
                validator,
                catalogReader,
                newCluster(catalogReader.getTypeFactory()),
                frameworkConfig.getConvertletTable(),
                frameworkConfig.getSqlToRelConverterConfig());

        return converter.convertQuery(sqlNode, false, true);
    }

    private static String explainPlan(RelNode node) {
        final String plan = RelOptUtil.dumpPlan("Debug Plan", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
        return plan;
    }
}
