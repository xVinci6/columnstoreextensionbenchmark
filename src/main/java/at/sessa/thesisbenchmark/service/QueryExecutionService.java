package at.sessa.thesisbenchmark.service;

import at.sessa.thesisbenchmark.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.lang.Nullable;
import org.springframework.util.StreamUtils;

import javax.sql.DataSource;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutionService {
    Logger logger = LoggerFactory.getLogger(QueryExecutionService.class);

    private final JdbcTemplate jdbcTemplate;

    public QueryExecutionService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.jdbcTemplate.setFetchSize(1);
        this.jdbcTemplate.setQueryTimeout(1200);
    }

    public void benchmark(String databaseVendor, String databaseType) {
        Result result = new Result(databaseVendor + "/" + databaseType);

        long startTime = System.currentTimeMillis();
        logger.info("Starting benchmark for {} on {}", databaseVendor, startTime);

        for(int i = 1; i <= 22; i++) {
            result.addQueryExecutionTime(executeBenchmark(databaseVendor, i, Result.ResultType.COLD));
            result.addQueryExecutionTime(executeBenchmark(databaseVendor, i, Result.ResultType.HOT));
        }

        long endTime = System.currentTimeMillis();
        logger.info("Ended benchmark for {} on {}", databaseVendor, endTime);
        logger.info(result.toString());
    }

    private Result.ResultTuple executeBenchmark(String databaseVendor, int i, Result.ResultType resultType) {
        String query = "";
        String classPathLocation = "classpath:db/queries/"+databaseVendor+"/"+i+".sql";
        try {
            ResourceLoader resourceLoader = new DefaultResourceLoader();
            Resource resource = resourceLoader.getResource(classPathLocation);
            query = StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("Error executing query {}", i, e);
        }
        long queryStartTime = System.currentTimeMillis();
        logger.info("Starting query {} on {}", i, queryStartTime);
        if(databaseVendor.equals("mssql") && i == 15) {
            createViewForMssqlQuery15();
        } try {
            jdbcTemplate.execute(query);
        } catch (DataAccessException e) {
            logger.error("Exception occurred during query execution, probably timed out", e);
        }
        if(databaseVendor.equals("mssql") && i == 15) {
            dropViewForMssqlQuery15();
        }
        long queryEndTime = System.currentTimeMillis();
        logger.info("Ended query {} on {}", i, queryEndTime);
        long queryDuration = queryEndTime - queryStartTime;
        logger.info("Duration of query {} on {}", i, queryDuration);
        return new Result.ResultTuple(i, queryDuration, resultType);
    }

    private void createViewForMssqlQuery15() {
        String query = "create view revenue0 (supplier_no, total_revenue) as\n" +
                "\tselect\n" +
                "\t\tl_suppkey,\n" +
                "\t\tsum(l_extendedprice * (1 - l_discount))\n" +
                "\tfrom\n" +
                "\t\tlineitem\n" +
                "\twhere\n" +
                "\t\tl_shipdate >= CAST('1996-01-01' AS datetime)\n" +
                "\t\tand l_shipdate < DATEADD(MONTH, 3, '1996-01-01')\n" +
                "\tgroup by\n" +
                "\t\tl_suppkey;";

        jdbcTemplate.execute(query);
    }

    private void dropViewForMssqlQuery15() {
        String query = "drop view revenue0;";

        jdbcTemplate.execute(query);
    }
}
