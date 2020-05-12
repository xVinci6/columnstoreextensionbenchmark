package at.sessa.thesisbenchmark.service;

import at.sessa.thesisbenchmark.configuration.MssqlProperties;
import at.sessa.thesisbenchmark.configuration.PostgresProperties;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;

@Service
public class BenchmarkSetupService {
    Logger logger = LoggerFactory.getLogger(BenchmarkSetupService.class);

    private final PostgresProperties postgresProperties;
    private final MssqlProperties mssqlProperties;
    private final String postgresRowContainerName = "postgres12";
    private final String postgresColumnContainerName = "postgres12cstore";
    private final String mssqlContainerName = "mssql";

    public BenchmarkSetupService(PostgresProperties postgresProperties, MssqlProperties mssqlProperties) {
        this.postgresProperties = postgresProperties;
        this.mssqlProperties = mssqlProperties;
    }

    /*
    @Bean
    @ConfigurationProperties(prefix="spring.datasource.mssql")
    @Primary
    public DataSource mssqlDataSource() {
        return DataSourceBuilder.create().build();
    }
    */

    public DataSource setupPostgresRowDatasource() {
        startPostgresRowContainer();
        DataSource postgresRowDataSource = createPostgresRowDataSource();
        waitUntilDatasourceReady(postgresRowDataSource, null);
        migratePostgresRow(postgresRowDataSource);
        return postgresRowDataSource;
    }

    public DataSource setupPostgresColumnDatasource() {
        startPostgresColumnContainer();
        DataSource postgresColumnDataSource = createPostgresColumnDataSource();
        waitUntilDatasourceReady(postgresColumnDataSource, null);
        migratePostgresColumn(postgresColumnDataSource);
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
        try {
            Runtime.getRuntime().exec(String.format("docker run -d -p 5432:5432 --name %s -e POSTGRES_PASSWORD=password postgres:12.2", postgresRowContainerName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startPostgresColumnContainer() {
        try {
            Runtime.getRuntime().exec(String.format("docker run -d -p 5432:5432 --name %s -e POSTGRES_PASSWORD=password postgres_12_cstore", postgresColumnContainerName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startMssqlContainer() {
        try {
            Runtime.getRuntime().exec(String.format("docker run --name %s -e \"ACCEPT_EULA=Y\" -e \"SA_PASSWORD=Password1\" -p 1433:1433 -v \"C:\\Users\\capta\\Desktop\\Thesis Rowstore\\Project\\6fea020c-cfa9-4790-8f53-a9e590cc6ed9-tpc-h-tool\\2.18.0_rc2\\dbgen\\testdata:/testdata\" -d mcr.microsoft.com/mssql/server:2019-CU4-ubuntu-16.04", mssqlContainerName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createMssqlDatabase() {
        try {
            Runtime.getRuntime().exec("sqlcmd -U sa -P Password1 -Q \"CREATE DATABASE springbootdb;\"");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DataSource createPostgresRowDataSource() {
        return DataSourceBuilder.create()
                .username(postgresProperties.getUsername())
                .password(postgresProperties.getPassword())
                .url(postgresProperties.getJdbcUrl()).build();
    }

    private DataSource createPostgresColumnDataSource() {
        return DataSourceBuilder.create()
                .username(postgresProperties.getUsername())
                .password(postgresProperties.getPassword())
                .url(postgresProperties.getJdbcUrl()).build();
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