# Data Migration Application Documentation

## Overview
This Spring Batch application migrates data from large CSV files to a PostgreSQL database with automatic handling of dynamic column structures. It features parallel processing capabilities, performance optimizations, and job control mechanisms.

## Key Features
- Dynamic schema handling (auto-creates tables based on CSV headers)
- Multi-threaded processing for improved performance
- REST API for job monitoring and control
- Progress tracking and status monitoring
- Index creation on key columns

## Architecture

### Core Components
1. **BatchConfig**: Central configuration for Spring Batch jobs
2. **FlatFileItemReader**: Reads CSV files with dynamic columns
3. **JdbcBatchItemWriter**: Writes data to PostgreSQL in efficient batches
4. **BatchController**: REST endpoints for job management
5. **BatchProgressListener**: Tracks processing progress

## Configuration

### Required Properties
Edit `application.yml` to configure:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/your_database
    username: your_username
    password: your_password
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 50
      minimum-idle: 10
      connection-timeout: 60000
  batch:
    jdbc:
      initialize-schema: never
    job:
      enabled: false

csv:
  input:
    file: "/path/to/your/large-file.csv"
```

### Important Parameters

| Parameter | Description | Default | Recommendation |
|-----------|-------------|---------|---------------|
| `csv.input.file` | Path to CSV file | N/A | Use absolute path |
| `CHUNK_SIZE` | Records per transaction | 10000 | Adjust based on memory |
| `spring.datasource.hikari.maximum-pool-size` | Connection pool size | 50 | Adjust based on resources |

## Database Setup

The application automatically:
1. Creates tables with column names from CSV headers
2. Creates indexes on the first column and any column with "id" in the name
3. Handles large datasets efficiently through batched inserts

## Running the Application

### Prerequisites
- Java 17+
- PostgreSQL database
- Sufficient disk space and memory for large files

### Starting the Application
```bash
# Build the application
mvn clean package

# Run with specific parameters
java -Xmx4g -jar target/datamigration-0.0.1-SNAPSHOT.jar
```

### REST API
API Endpoint : `http://localhost:8082/`
- Start job: `POST /api/batch/start`
- Check status: `GET /api/batch/status`
- Stop job: `POST /api/batch/stop`

## Troubleshooting

### Common Issues

1. **Duplicate Schema Objects**
    - Error: `relation "batch_step_execution_seq" already exists`
    - Solution: Set `spring.batch.jdbc.initialize-schema=never`

2. **Duplicate Job Registration**
    - Error: `A job configuration with this name [importJob] was already registered`
    - Solution: Ensure only one bean with the same job name exists or modify job registry configuration

3. **ON CONFLICT Errors**
    - Error: Error when using ON CONFLICT clauses
    - Solution: Ensure unique constraints exist on referenced columns

4. **Memory Issues**
    - If experiencing OutOfMemoryErrors, adjust JVM heap size and reduce chunk size

## Performance Optimization

1. Adjust `CHUNK_SIZE` based on your dataset characteristics
2. Configure thread pool size based on your CPU cores
3. Fine-tune Hikari connection pool parameters:
   ```yaml
   spring:
     datasource:
       hikari:
         maximum-pool-size: 50  # Adjust based on CPU cores
         minimum-idle: 10       # For sustained performance
         connection-timeout: 60000
         idle-timeout: 600000
         max-lifetime: 1800000
   ```

## Best Practices

1. Always back up your database before large migrations
2. Run a small test batch before processing the entire dataset
3. Monitor database and application performance during migration
4. Use the status API to track progress of long-running jobs

## Extending the Application

To adapt this application for different data sources:
1. Create a new reader implementation in BatchConfig
2. Adjust table creation logic if needed
3. Modify writer implementation for your target database

## Security Considerations

1. Secure database credentials in production deployments
2. Use environment variables or a secret manager instead of hardcoded credentials
3. Consider adding authentication to REST endpoints

## References

* [Spring Batch Documentation](https://docs.spring.io/spring-batch/docs/current/reference/html/)
* [PostgreSQL Documentation](https://www.postgresql.org/docs/)
* [Spring Boot Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/)
