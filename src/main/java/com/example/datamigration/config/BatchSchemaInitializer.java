// src/main/java/com/example/datamigration/config/BatchSchemaInitializer.java
package com.example.datamigration.config;

import jakarta.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

@Configuration
public class BatchSchemaInitializer {

    private static final Logger logger = LoggerFactory.getLogger(BatchSchemaInitializer.class);

    @Autowired
    private DataSource dataSource;

    @PostConstruct
    public void initializeSchema() {
        logger.info("Explicitly initializing Spring Batch schema");
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-postgresql.sql"));
        populator.setIgnoreFailedDrops(true);

        try {
            populator.execute(dataSource);
            logger.info("Spring Batch schema initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize Spring Batch schema", e);
        }
    }
}