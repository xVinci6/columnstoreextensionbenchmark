package at.sessa.thesisbenchmark.service;

import at.sessa.thesisbenchmark.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.lang.Nullable;
import org.springframework.util.ResourceUtils;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutionService {
    Logger logger = LoggerFactory.getLogger(QueryExecutionService.class);

    private final JdbcTemplate jdbcTemplate;

    public QueryExecutionService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void benchmark(String databaseVendor, String databaseType) {
        Result result = new Result(databaseVendor + "/" + databaseType);

        long startTime = System.currentTimeMillis();
        logger.info("Starting benchmark for {} on {}", databaseVendor, startTime);

        for(int i = 1; i <= 22; i++) {
            String query = "";
            String classPathLocation = "classpath:db/queries/"+databaseVendor+"/"+i+".sql";
            try {
                File sqlFile = ResourceUtils.getFile(classPathLocation);
                query = new String(Files.readAllBytes(sqlFile.toPath()));
            } catch (Exception e) {
                logger.error("Error executing query {}", i, e);
            }
            long queryStartTime = System.currentTimeMillis();
            logger.info("Starting query {} on {}", i, queryStartTime);
            jdbcTemplate.execute(new ExecuteStatementCallback(query));
            long queryEndTime = System.currentTimeMillis();
            logger.info("Ended query {} on {}", i, queryEndTime);
            long queryDuration = queryEndTime - queryStartTime;
            logger.info("Duration of query {} on {}", i, queryDuration);
            result.addQueryExecutionTime(i, queryDuration);
        }

        long endTime = System.currentTimeMillis();
        logger.info("Ended benchmark for {} on {}", databaseVendor, endTime);
        logger.info(result.toString());
    }

    static class ExecuteStatementCallback implements StatementCallback<Object>, SqlProvider {
        private final String sql;

        public ExecuteStatementCallback(String sql) {
            this.sql = sql;
        }

        @Override
        @Nullable
        public Object doInStatement(Statement stmt) throws SQLException {
            stmt.setFetchSize(1);
            stmt.execute(sql);
            return null;
        }
        @Override
        public String getSql() {
            return sql;
        }
    }
}
