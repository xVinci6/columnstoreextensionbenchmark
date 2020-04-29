package at.sessa.thesisbenchmark.configuration;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class PersistenceConfiguration {
    Logger logger = LoggerFactory.getLogger(PersistenceConfiguration.class);

    /*
    @Bean
    @ConfigurationProperties(prefix="spring.datasource.mssql")
    @Primary
    public DataSource mssqlDataSource() {
        return DataSourceBuilder.create().build();
    }
    */
    @Bean
    @ConfigurationProperties(prefix="spring.datasource.postgres.column")
    public DataSource postgresColumnDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix="spring.datasource.postgres.row")
    public DataSource postgresRowDataSource() {
        return DataSourceBuilder.create().build();
    }

    @PostConstruct
    public void migrateFlyway() {
        //migrateMssql();
        migratePostgres();
    }

    /*
    private void migrateMssql() {
        logger.info("Initializing mssql row migration");
        Flyway.configure().locations("db/migration/mssql/row").dataSource(mssqlDataSource()).load();

        logger.info("Initializing mssql column migration");
        Flyway.configure().locations("db/migration/mssql/column").dataSource(mssqlDataSource()).load();
    }
    */
    private void migratePostgres() {
        logger.info("Initializing postgres row migration");
        Flyway.configure().locations("db/migration/postgres/row").dataSource(postgresRowDataSource()).load().migrate();

        logger.info("Initializing postgres column migration");
        Flyway.configure().locations("db/migration/postgres/column").dataSource(postgresColumnDataSource()).load().migrate();
    }
}