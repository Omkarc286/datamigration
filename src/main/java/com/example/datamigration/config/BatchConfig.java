package com.example.datamigration.config;

import com.example.datamigration.listener.BatchProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobOperator;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CharsetEncoder;

@Configuration
@EnableBatchProcessing(isolationLevelForCreate = "ISOLATION_DEFAULT") // Add isolation level to avoid conflicts
public class BatchConfig {
    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);
    private static final int CHUNK_SIZE = 10000;
    private final BatchProgressListener batchProgressListener;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Value("${csv.input.file}")
    private String csvFilePath;

    public BatchConfig(JobRepository jobRepository,
                       PlatformTransactionManager transactionManager,
                       DataSource dataSource,
                       BatchProgressListener batchProgressListener) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.dataSource = dataSource;
        this.batchProgressListener = batchProgressListener;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("batch-");
        executor.setKeepAliveSeconds(120);
        executor.initialize();
        return executor;
    }
    @Bean
    public JobExecutionListener jobListener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                logger.info("Starting CSV import job");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                logger.info("CSV import completed with status: {}", jobExecution.getStatus());

                // Fix: Calculate duration properly with LocalDateTime
                if (jobExecution.getEndTime() != null && jobExecution.getStartTime() != null) {
                    long durationSeconds = java.time.Duration.between(
                            jobExecution.getStartTime(),
                            jobExecution.getEndTime()
                    ).getSeconds();
                    logger.info("Time taken: {} seconds", durationSeconds);
                }
            }
        };
    }

    @Bean
    public Job importJob() throws Exception {
        return new JobBuilder("importJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobListener())
                .start(importStep())
                .build();
    }
    @Bean
    public JobRegistry jobRegistry() {
        return new MapJobRegistry();
    }

    /*
    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry);
        return postProcessor;
    }
    */
    @Bean
    public JobOperator jobOperator(JobLauncher jobLauncher,
                                   JobRepository jobRepository,
                                   JobRegistry jobRegistry,
                                   JobExplorer jobExplorer) {
        SimpleJobOperator operator = new SimpleJobOperator();
        operator.setJobLauncher(jobLauncher);
        operator.setJobRepository(jobRepository);
        operator.setJobRegistry(jobRegistry);
        operator.setJobExplorer(jobExplorer);
        return operator;
    }
    @Bean
    public Step importStep() throws Exception {
        return new StepBuilder("importStep", jobRepository)
                .<Map<String, String>, Map<String, String>>chunk(CHUNK_SIZE, transactionManager)
                .reader(reader())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .listener(batchProgressListener)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Map<String, String>> reader() throws Exception {
        List<String> headers = readHeaders(csvFilePath);
        logger.info("Found {} columns in CSV", headers.size());

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(headers.toArray(new String[0]));
        tokenizer.setStrict(false);

        DefaultLineMapper<Map<String, String>> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> {
            Map<String, String> row = new HashMap<>(headers.size());
            for (String header : headers) {
                row.put(header, fieldSet.readString(header));
            }
            return row;
        });

        return new FlatFileItemReaderBuilder<Map<String, String>>()
                .name("csvReader")
                .resource(new FileSystemResource(csvFilePath))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .saveState(false) // For better performance
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Map<String, String>> writer() throws Exception {
        List<String> headers = readHeaders(csvFilePath);
        String tableName = "csv_data";

        createTableIfNotExists(tableName, headers);
        createIndexes(tableName, headers);
        
        // Create a UTF-8 encoder for validation
        CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder();

        return new JdbcBatchItemWriterBuilder<Map<String, String>>()
                .sql(generateInsertSql(tableName, headers))
                .itemSqlParameterSourceProvider(item -> {
                    MapSqlParameterSource paramSource = new MapSqlParameterSource();
                    for (Map.Entry<String, String> entry : item.entrySet()) {
                        String value = entry.getValue();
                        // Check if the value is valid UTF-8
                        if (value != null && !isValidUtf8(value, utf8Encoder)) {
                            logger.warn("Non-UTF-8 characters detected in column '{}', sanitizing value", entry.getKey());
                            // Replace with sanitized version (this removes invalid chars)
                            value = sanitizeToUtf8(value);
                        }
                        paramSource.addValue(entry.getKey(), value);
                    }
                    return paramSource;
                })
                .dataSource(dataSource)
                .build();
    }
    
    /**
     * Checks if a string contains valid UTF-8 characters
     */
    private boolean isValidUtf8(String text, CharsetEncoder encoder) {
        return encoder.canEncode(text);
    }
    
    /**
     * Sanitizes a string to ensure it only contains valid UTF-8 characters
     */
    private String sanitizeToUtf8(String text) {
        if (text == null) return null;
        
        StringBuilder sanitized = new StringBuilder();
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
        
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (encoder.canEncode(c)) {
                sanitized.append(c);
            } else {
                // Replace invalid character with a replacement character or empty string
                sanitized.append("�"); // Unicode replacement character
            }
        }
        
        return sanitized.toString();
    }

    private void createTableIfNotExists(String tableName, List<String> headers) throws Exception {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
        sql.append("id SERIAL PRIMARY KEY, ");

        for (String header : headers) {
            sql.append("\"").append(header.replace("\"", "")).append("\" TEXT, ");
        }

        sql.setLength(sql.length() - 2);
        sql.append(")");

        logger.info("Creating table with SQL: {}", sql);
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(sql.toString());
        }
    }

    private void createIndexes(String tableName, List<String> headers) {
        // Create indexes on likely key columns (first column or columns with "id" in name)
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement()) {

            // Create index on first column
            if (!headers.isEmpty()) {
                String firstCol = headers.get(0).replace("\"", "");
                String indexSql = "CREATE INDEX IF NOT EXISTS idx_" + tableName + "_" + firstCol.toLowerCase() +
                        " ON " + tableName + " (\"" + firstCol + "\")";
                statement.execute(indexSql);
                logger.info("Created index on first column: {}", firstCol);
            }

            // Create indexes on potential ID columns
            for (String header : headers) {
                String cleanHeader = header.replace("\"", "");
                if (cleanHeader.toLowerCase().contains("id")) {
                    String indexSql = "CREATE INDEX IF NOT EXISTS idx_" + tableName + "_" + cleanHeader.toLowerCase() +
                            " ON " + tableName + " (\"" + cleanHeader + "\")";
                    statement.execute(indexSql);
                    logger.info("Created index on ID column: {}", cleanHeader);
                }
            }
        } catch (Exception e) {
            logger.error("Error creating indexes", e);
        }
    }

    private String generateInsertSql(String tableName, List<String> headers) {
        StringBuilder fields = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (String header : headers) {
            fields.append("\"").append(header.replace("\"", "")).append("\", ");
            values.append(":").append(header.replace("\"", "")).append(", ");
        }

        fields.setLength(fields.length() - 2);
        values.setLength(values.length() - 2);

        return "INSERT INTO " + tableName + " (" + fields + ") VALUES (" + values + ")";
    }

    private List<String> readHeaders(String path) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IllegalStateException("CSV file is empty");
            }
            return Arrays.asList(headerLine.split(","));
        }
    }
}
