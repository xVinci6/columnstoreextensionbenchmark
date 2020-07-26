package at.sessa.thesisbenchmark.service;

import at.sessa.thesisbenchmark.Utility;
import at.sessa.thesisbenchmark.configuration.GenericProperties;
import at.sessa.thesisbenchmark.configuration.MssqlProperties;
import at.sessa.thesisbenchmark.configuration.PostgresProperties;
import org.flywaydb.core.Flyway;
import org.postgresql.Driver;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;

@Service
public class BenchmarkSetupService {
    Logger logger = LoggerFactory.getLogger(BenchmarkSetupService.class);

    private final PostgresProperties postgresProperties;
    private final MssqlProperties mssqlProperties;
    private final GenericProperties genericProperties;
    private final String postgresRowContainerName = "postgres12";
    private final String postgresColumnContainerName = "postgres12cstore";
    private final String mssqlContainerName = "mssql";
    private final String dockerTestdataMountpath = "\"C:\\Users\\capta\\Desktop\\Thesis Rowstore\\Project\\columnstoreextensionbenchmark\\prerequisites\\testdata:/testdata\"";
    private final String testDataLocationInContainer;

    public BenchmarkSetupService(PostgresProperties postgresProperties, MssqlProperties mssqlProperties, GenericProperties genericProperties) {
        this.postgresProperties = postgresProperties;
        this.mssqlProperties = mssqlProperties;
        this.genericProperties = genericProperties;
        testDataLocationInContainer = "/testdata/"+genericProperties.getScaleFactor()+"/";
    }

    public DataSource setupPostgresRowDatasource() {
        startPostgresRowContainer();
        DataSource postgresRowDataSource = createPostgresDataSource();
        waitUntilDatasourceReady(postgresRowDataSource, null);
        migratePostgresRow(postgresRowDataSource);
        loadPostgresData(postgresRowDataSource);
        try {
            PgConnection pgConnection = (PgConnection) postgresRowDataSource.getConnection();
            pgConnection.setDefaultFetchSize(1);
        } catch (SQLException exception) {
            logger.error("Error while setting fetch size", exception);
        }
        return postgresRowDataSource;
    }

    public DataSource setupPostgresColumnDatasource() {
        startPostgresColumnContainer();
        DataSource postgresColumnDataSource = createPostgresDataSource();
        waitUntilDatasourceReady(postgresColumnDataSource, null);
        migratePostgresColumn(postgresColumnDataSource);
        loadPostgresData(postgresColumnDataSource);
        try {
            PgConnection pgConnection = (PgConnection) postgresColumnDataSource.getConnection();
            pgConnection.setDefaultFetchSize(1);
        } catch (SQLException exception) {
            logger.error("Error while setting fetch size", exception);
        }
        return postgresColumnDataSource;
    }

    public DataSource setupMssqlRowDataSource() {
        startMssqlContainer();
        DataSource mssqlDataSource = createMssqlDataSource();
        waitUntilDatasourceReady(mssqlDataSource, this::createMssqlDatabase);
        migrateMssqlRow(mssqlDataSource);
        return mssqlDataSource;
    }

    public DataSource setupMssqlColumnDataSource() {
        startMssqlContainer();
        DataSource mssqlDataSource = createMssqlDataSource();
        waitUntilDatasourceReady(mssqlDataSource, this::createMssqlDatabase);
        migrateMssqlColumn(mssqlDataSource);
        return mssqlDataSource;
    }

    public void cleanUpContainers() {
        logger.info("Cleaning up containers");
        cleanUpPostgresRowContainer();
        cleanUpPostgresColumnContainer();
        cleanUpMssqlContainer();
    }

    public void cleanUpPostgresRowContainer() {
        try {
            logger.info("Cleaning up postgresrowcontainer");
            Runtime.getRuntime().exec(String.format("docker stop %s", postgresRowContainerName));
            Thread.sleep(20000);
            Runtime.getRuntime().exec(String.format("docker rm %s", postgresRowContainerName));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanUpPostgresColumnContainer() {
        try {
            logger.info("Cleaning up postgrescolumncontainer");
            Runtime.getRuntime().exec(String.format("docker stop %s", postgresColumnContainerName));
            Thread.sleep(20000);
            Runtime.getRuntime().exec(String.format("docker rm %s", postgresColumnContainerName));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanUpMssqlContainer() {
        try {
            logger.info("Cleaning up mssqlcontainer");
            Runtime.getRuntime().exec(String.format("docker stop %s", mssqlContainerName));
            Thread.sleep(20000);
            Runtime.getRuntime().exec(String.format("docker rm %s", mssqlContainerName));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void startPostgresRowContainer() {
        Utility.execRuntime("docker volume create postgres");
        Utility.execRuntime(String.format("docker run -d -p 5432:5432 --shm-size=1g --name %s -e POSTGRES_PASSWORD=password -e PGDATA=/var/lib/postgresql/data/pgdata -v %s --mount source=postgres,target=/var/lib/postgresql/data postgres:12.2", postgresRowContainerName, dockerTestdataMountpath));
    }

    private void startPostgresColumnContainer() {
        Utility.execRuntime("docker volume create postgrescolumn");
        Utility.execRuntime(String.format("docker run -d -p 5432:5432 --shm-size=1g --name %s -e POSTGRES_PASSWORD=password -e PGDATA=/var/lib/postgresql/data/pgdata -v %s --mount source=postgrescolumn,target=/var/lib/postgresql/data postgres_12_cstore", postgresColumnContainerName, dockerTestdataMountpath));
    }

    private void startMssqlContainer() {
        Utility.execRuntime(String.format("docker run --name %s -e \"ACCEPT_EULA=Y\" -e \"SA_PASSWORD=Password1\" -p 1433:1433 -v %s -d mcr.microsoft.com/mssql/server:2019-CU4-ubuntu-16.04", mssqlContainerName, dockerTestdataMountpath));
    }

    private void createMssqlDatabase() {
        try {
            Runtime.getRuntime().exec("sqlcmd -U sa -P Password1 -Q \"CREATE DATABASE springbootdb;\"");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DataSource createPostgresDataSource() {
        SimpleDriverDataSource simpleDriverDataSource = new SimpleDriverDataSource();
        simpleDriverDataSource.setDriverClass(Driver.class);
        simpleDriverDataSource.setUrl(postgresProperties.getJdbcUrl());
        simpleDriverDataSource.setUsername(postgresProperties.getUsername());
        simpleDriverDataSource.setPassword(postgresProperties.getPassword());
        return simpleDriverDataSource;
    }

    private DataSource createMssqlDataSource() {
        return DataSourceBuilder.create()
                .username(mssqlProperties.getUsername())
                .password(mssqlProperties.getPassword())
                .url(mssqlProperties.getJdbcUrl())
                .driverClassName(mssqlProperties.getDriverClassName())
                .build();
    }

    private void migratePostgresRow(DataSource dataSource) {
        logger.info("Initializing postgres row migration");
        Flyway.configure().locations("db/migration/postgres/row").dataSource(dataSource).load().migrate();
    }

    private void migratePostgresColumn(DataSource dataSource) {
        logger.info("Initializing postgres column migration");
        Flyway.configure().locations("db/migration/postgres/column").dataSource(dataSource).load().migrate();
    }

    private void migrateMssqlRow(DataSource dataSource) {
        logger.info("Initializing mssql row migration");
        Flyway.configure().locations("db/migration/mssql/row").dataSource(dataSource).load().migrate();
    }

    private void migrateMssqlColumn(DataSource dataSource) {
        logger.info("Initializing mssql column migration");
        Flyway.configure().locations("db/migration/mssql/column").dataSource(dataSource).load().migrate();
    }

    public void loadPostgresData(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        long startTime = System.currentTimeMillis();

        logger.info("Started import at: {}", startTime);

        jdbcTemplate.execute(
                "COPY REGION(R_REGIONKEY, R_NAME, R_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"region.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading regions at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY NATION(N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"nation.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading nations at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY SUPPLIER(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"supplier.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading suppliers at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"part.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading parts at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY CUSTOMER(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT)\n" +
                "FROM '"+testDataLocationInContainer+"customer.tbl'\n" +
                "DELIMITER '|'"
        );

        logger.info("Finished loading customers at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"partsupp.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading partsuppliers at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"orders.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading orders at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "COPY LINEITEM(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"lineitem.tbl'\n" +
                        "DELIMITER '|'"
        );

        logger.info("Finished loading lineitems at: {}", System.currentTimeMillis());

        String classPathLocation = "classpath:db/migration/postgres/keys.sql";
        try {
            File sqlFile = ResourceUtils.getFile(classPathLocation);
            String query = new String(Files.readAllBytes(sqlFile.toPath()));
            jdbcTemplate.execute(query);
        } catch (Exception e) {
            logger.error("Error adding keys", e);
        }

        logger.info("Finished adding keys and constraints at: {}", System.currentTimeMillis());

        jdbcTemplate.execute("VACUUM(ANALYZE) REGION");
        jdbcTemplate.execute("VACUUM(ANALYZE) NATION");
        jdbcTemplate.execute("VACUUM(ANALYZE) SUPPLIER");
        jdbcTemplate.execute("VACUUM(ANALYZE) PART");
        jdbcTemplate.execute("VACUUM(ANALYZE) CUSTOMER");
        jdbcTemplate.execute("VACUUM(ANALYZE) PARTSUPP");
        jdbcTemplate.execute("VACUUM(ANALYZE) ORDERS");
        jdbcTemplate.execute("VACUUM(ANALYZE) LINEITEM");

        logger.info("Finished vacuum analyze at: {}", System.currentTimeMillis());

        long endTime = System.currentTimeMillis();

        logger.info("Ended import at: {}", endTime);
        logger.info("Total import duration: {}", endTime - startTime);
    }

    public void loadMsSqlData(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute(
                "BULK INSERT CUSTOMER\n" +
                        "FROM "+testDataLocationInContainer+"customer.tbl'\n"+
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n')"
        );

        jdbcTemplate.execute(
                "COPY PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"partsupp.tbl'\n" +
                        "DELIMITER '|'"
        );

        jdbcTemplate.execute(
                "COPY ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"orders.tbl'\n" +
                        "DELIMITER '|'"
        );

        jdbcTemplate.execute(
                "COPY LINEITEM(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"lineitem.tbl'\n" +
                        "DELIMITER '|'"
        );

        jdbcTemplate.execute(
                "COPY NATION(N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"nation.tbl'\n" +
                        "DELIMITER '|'"
        );

        jdbcTemplate.execute(
                "COPY REGION(R_REGIONKEY, R_NAME, R_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"region.tbl'\n" +
                        "DELIMITER '|'"
        );

        jdbcTemplate.execute(
                "COPY SUPPLIER(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"supplier.tbl'\n" +
                        "DELIMITER '|'"
        );

        jdbcTemplate.execute(
                "COPY PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT)\n" +
                        "FROM '"+testDataLocationInContainer+"supplier.tbl'\n" +
                        "DELIMITER '|'"
        );
    }

    private void waitUntilDatasourceReady(DataSource dataSource, Runnable runnable) {
        try {
            dataSource.getConnection();
        } catch (Exception e) {
            try {
                logger.info("Waiting for datasource to become ready");
                Thread.sleep(20000);
                if(runnable != null) {
                    runnable.run();
                }
                waitUntilDatasourceReady(dataSource, runnable);
            } catch (InterruptedException interruptedException) {
                throw new RuntimeException(interruptedException);
            }
        }
    }
}