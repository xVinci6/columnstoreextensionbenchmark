package at.sessa.thesisbenchmark.service;

import at.sessa.thesisbenchmark.Utility;
import at.sessa.thesisbenchmark.configuration.GenericProperties;
import at.sessa.thesisbenchmark.configuration.MssqlProperties;
import at.sessa.thesisbenchmark.configuration.PostgresProperties;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.flywaydb.core.Flyway;
import org.postgresql.Driver;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Service
public class BenchmarkSetupService {
    Logger logger = LoggerFactory.getLogger(BenchmarkSetupService.class);

    private final PostgresProperties postgresProperties;
    private final MssqlProperties mssqlProperties;
    private final GenericProperties genericProperties;
    private final String postgresRowContainerName = "postgres12";
    private final String postgresColumnContainerName = "postgres12cstore";
    private final String mssqlContainerName = "mssql";
    private final String dockerTestdataMountpath;
    private final String testDataLocationInContainer;

    public BenchmarkSetupService(PostgresProperties postgresProperties, MssqlProperties mssqlProperties, GenericProperties genericProperties) {
        this.postgresProperties = postgresProperties;
        this.mssqlProperties = mssqlProperties;
        this.genericProperties = genericProperties;
        testDataLocationInContainer = "/testdata/"+genericProperties.getScaleFactor()+"/";
        dockerTestdataMountpath = genericProperties.getDockerTestdataMountPath() + ":/testdata";
    }

    public DataSource setupPostgresRowDatasource() {
        startPostgresRowContainer();
        DataSource postgresRowDataSource = createPostgresDataSource();
        waitUntilDatasourceReady(postgresRowDataSource, null);
        migratePostgresRow(postgresRowDataSource);
        loadPostgresData(postgresRowDataSource);
        restartPostgresRowContainer();
        postgresRowDataSource = createPostgresDataSource();
        waitUntilDatasourceReady(postgresRowDataSource, null);
        printVolumeSizes();
        return postgresRowDataSource;
    }

    public DataSource setupPostgresColumnDatasource() {
        startPostgresColumnContainer();
        DataSource postgresColumnDataSource = createPostgresDataSource();
        waitUntilDatasourceReady(postgresColumnDataSource, null);
        migratePostgresColumn(postgresColumnDataSource);
        loadPostgresData(postgresColumnDataSource);
        restartPostgresColContainer();
        postgresColumnDataSource = createPostgresDataSource();
        waitUntilDatasourceReady(postgresColumnDataSource, null);
        printVolumeSizes();
        return postgresColumnDataSource;
    }

    public DataSource setupMssqlRowDataSource() {
        startMssqlContainer("mssqlrow");
        DataSource mssqlDataSource = createMssqlDataSource();
        waitUntilDatasourceReady(mssqlDataSource, this::createMssqlDatabase);
        migrateMssqlRow(mssqlDataSource);
        loadMsSqlData(mssqlDataSource, "row");
        restartMssqlRowContainer();
        mssqlDataSource = createMssqlDataSource();
        waitUntilDatasourceReady(mssqlDataSource, null);
        printVolumeSizes();
        return mssqlDataSource;
    }

    public DataSource setupMssqlColumnDataSource() {
        startMssqlContainer("mssqlcolumn");
        DataSource mssqlDataSource = createMssqlDataSource();
        waitUntilDatasourceReady(mssqlDataSource, this::createMssqlDatabase);
        migrateMssqlColumn(mssqlDataSource);
        loadMsSqlData(mssqlDataSource, "column");
        restartMssqlColumnContainer();
        mssqlDataSource = createMssqlDataSource();
        waitUntilDatasourceReady(mssqlDataSource, null);
        printVolumeSizes();
        return mssqlDataSource;
    }

    public void cleanUpContainers() {
        logger.info("Cleaning up containers");
        cleanUpPostgresRowContainer();
        cleanUpPostgresColumnContainer();
        cleanUpMssqlContainer("mssqlrow");
        cleanUpMssqlContainer("mssqlcolumn");
    }

    public void cleanUpPostgresRowContainer() {
        try {
            logger.info("Cleaning up postgresrowcontainer");
            Runtime.getRuntime().exec(String.format("docker stop %s", postgresRowContainerName));
            Thread.sleep(20000);
            Runtime.getRuntime().exec(String.format("docker rm %s", postgresRowContainerName));
            Runtime.getRuntime().exec(String.format("docker volume rm %s", "postgres"));
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
            Runtime.getRuntime().exec(String.format("docker volume rm %s", "postgrescolumn"));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanUpMssqlContainer(String volumeName) {
        try {
            logger.info("Cleaning up mssqlcontainer");
            Runtime.getRuntime().exec(String.format("docker stop %s", mssqlContainerName));
            Thread.sleep(20000);
            Runtime.getRuntime().exec(String.format("docker rm %s", mssqlContainerName));
            Runtime.getRuntime().exec(String.format("docker volume rm %s", volumeName));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void startPostgresRowContainer() {
        Utility.execRuntime("docker volume create postgres");
        Utility.execRuntime(String.format("docker run -d -p 5432:5432 --shm-size=4g --cpus=8 -m=32G --name %s -e POSTGRES_PASSWORD=password -e PGDATA=/var/lib/postgresql/data/pgdata -v %s --mount source=postgres,target=/var/lib/postgresql/data postgres:12.3 -c shared_buffers=8192MB -c effective_cache_size=163848MB -c work_mem=512MB", postgresRowContainerName, dockerTestdataMountpath));
    }

    private void restartPostgresRowContainer() {
        Utility.execRuntime("docker restart postgres12");
    }

    private void startPostgresColumnContainer() {
        Utility.execRuntime("docker volume create postgrescolumn");
        Utility.execRuntime(String.format("docker run -d -p 5432:5432 --shm-size=16g --cpus=8 -m=32g --name %s -e POSTGRES_PASSWORD=password -e PGDATA=/var/lib/postgresql/data/pgdata -v %s --mount source=postgrescolumn,target=/var/lib/postgresql/data postgres_12_cstore -c shared_buffers=8192MB -c effective_cache_size=163848MB -c work_mem=512MB", postgresColumnContainerName, dockerTestdataMountpath));
    }

    private void restartPostgresColContainer() {
        Utility.execRuntime("docker restart postgres12cstore");
    }

    private void startMssqlContainer(String volumeName) {
        Utility.execRuntime("docker volume create "+volumeName);
        Utility.execRuntime(String.format("docker run --name %s -e ACCEPT_EULA=Y -e SA_PASSWORD=Password1 -e MSSQL_MEMORY_LIMIT_MB=32768 -p 1433:1433 --shm-size=4g --cpus=8 -m=32g -v %s --mount source="+volumeName+",target=/var/opt/mssql -d mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-16.04", mssqlContainerName, dockerTestdataMountpath));
    }

    private void restartMssqlRowContainer() {
        Utility.execRuntime("docker restart mssql");
    }

    private void restartMssqlColumnContainer() {
        Utility.execRuntime("docker restart mssql");
    }

    private void createMssqlDatabase() {
        Utility.execRuntime(String.format("docker exec "+mssqlContainerName+" /opt/mssql-tools/bin/sqlcmd -U sa -P Password1 -Q %s","\\\"CREATE DATABASE springbootdb;\\\""));
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
        SimpleDriverDataSource simpleDriverDataSource = new SimpleDriverDataSource();
        simpleDriverDataSource.setDriverClass(SQLServerDriver.class);
        simpleDriverDataSource.setUrl(mssqlProperties.getJdbcUrl());
        simpleDriverDataSource.setUsername(mssqlProperties.getUsername());
        simpleDriverDataSource.setPassword(mssqlProperties.getPassword());
        return simpleDriverDataSource;
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
            ResourceLoader resourceLoader = new DefaultResourceLoader();
            Resource resource = resourceLoader.getResource(classPathLocation);
            String query= StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
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

    public void loadMsSqlData(DataSource dataSource, String databaseType) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        long startTime = System.currentTimeMillis();

        logger.info("Started import at: {}", startTime);

        jdbcTemplate.execute(
                "BULK INSERT REGION\n" +
                        "FROM '"+testDataLocationInContainer+"region.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading regions at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT NATION\n" +
                        "FROM '"+testDataLocationInContainer+"nation.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading nations at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT SUPPLIER\n" +
                        "FROM '"+testDataLocationInContainer+"supplier.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading suppliers at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT PART\n" +
                        "FROM '"+testDataLocationInContainer+"part.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading parts at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT CUSTOMER\n" +
                        "FROM '"+testDataLocationInContainer+"customer.tbl'\n"+
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading customers at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT PARTSUPP\n" +
                        "FROM '"+testDataLocationInContainer+"partsupp.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading partsuppliers at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT ORDERS\n" +
                        "FROM '"+testDataLocationInContainer+"orders.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading orders at: {}", System.currentTimeMillis());

        jdbcTemplate.execute(
                "BULK INSERT LINEITEM\n" +
                        "FROM '"+testDataLocationInContainer+"lineitem.tbl'\n" +
                        "WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '\\n', TABLOCK)"
        );

        logger.info("Finished loading lineitems at: {}", System.currentTimeMillis());

        String classPathLocation = "classpath:db/migration/mssql/"+databaseType+"/keys.sql";
        try {
            ResourceLoader resourceLoader = new DefaultResourceLoader();
            Resource resource = resourceLoader.getResource(classPathLocation);
            String query = StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
            jdbcTemplate.execute(query);
        } catch (Exception e) {
            logger.error("Error adding keys", e);
        }

        logger.info("Finished adding keys and constraints at: {}", System.currentTimeMillis());

        long endTime = System.currentTimeMillis();

        logger.info("Ended import at: {}", endTime);
        logger.info("Total import duration: {}", endTime - startTime);
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

    private void printVolumeSizes() {
        Utility.execRuntime("docker system df -v");
    }
}