package at.sessa.thesisbenchmark;

import at.sessa.thesisbenchmark.service.BenchmarkSetupService;
import at.sessa.thesisbenchmark.service.QueryExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import javax.sql.DataSource;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class ThesisbenchmarkApplication implements CommandLineRunner {
	Logger logger = LoggerFactory.getLogger(ThesisbenchmarkApplication.class);

	private final BenchmarkSetupService benchmarkSetupService;

	public ThesisbenchmarkApplication(BenchmarkSetupService benchmarkSetupService) {
		this.benchmarkSetupService = benchmarkSetupService;
	}

	public static void main(String[] args) {
		SpringApplication.run(ThesisbenchmarkApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		try {
			executeBenchmark();
		} catch (Exception e) {
			logger.error("Exception while benchmarking", e);
		} finally {
			benchmarkSetupService.cleanUpContainers();
		}
	}

	private void executeBenchmark() {
		QueryExecutionService queryExecutionService;

		logger.info("Starting postgres row datasource benchmark");
		DataSource postgresRowDatasource = benchmarkSetupService.setupPostgresRowDatasource();
		queryExecutionService = new QueryExecutionService(postgresRowDatasource);
		queryExecutionService.benchmark("postgres", "row");
		benchmarkSetupService.cleanUpPostgresRowContainer();

		logger.info("Starting postgres column datasource benchmark");
		DataSource postgresColumnDataSource = benchmarkSetupService.setupPostgresColumnDatasource();
		queryExecutionService = new QueryExecutionService(postgresColumnDataSource);
		queryExecutionService.benchmark("postgres", "column");
		benchmarkSetupService.cleanUpPostgresColumnContainer();

		logger.info("Starting mssql row datasource benchmark");
		DataSource mssqlRowDataSource = benchmarkSetupService.setupMssqlRowDataSource();
		queryExecutionService = new QueryExecutionService(mssqlRowDataSource);
		queryExecutionService.benchmark("mssql", "row");
		benchmarkSetupService.cleanUpMssqlContainer("mssqlrow");


		logger.info("Starting mssql column datasource benchmark");
		DataSource mssqlColumnDataSource = benchmarkSetupService.setupMssqlColumnDataSource();
		queryExecutionService = new QueryExecutionService(mssqlColumnDataSource);
		queryExecutionService.benchmark("mssql", "column");
		benchmarkSetupService.cleanUpMssqlContainer("mssqlcolumn");
	}
}
