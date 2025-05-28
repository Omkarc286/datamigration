// src/main/java/com/example/datamigration/listener/BatchProgressListener.java
package com.example.datamigration.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.stereotype.Component;

@Component
public class BatchProgressListener implements StepExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(BatchProgressListener.class);
    private long startTime;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        startTime = System.currentTimeMillis();
        log.info("Starting step: {}", stepExecution.getStepName());
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        long duration = System.currentTimeMillis() - startTime;
        log.info("===== Step Execution Summary =====");
        log.info("Step: {}", stepExecution.getStepName());
        log.info("Read count: {}", stepExecution.getReadCount());
        log.info("Write count: {}", stepExecution.getWriteCount());
        log.info("Filter count: {}", stepExecution.getFilterCount());
        log.info("Commit count: {}", stepExecution.getCommitCount());
        log.info("Skip count: {}", stepExecution.getSkipCount());
        log.info("Processing time: {} seconds", duration / 1000);
        log.info("=================================");
        return stepExecution.getExitStatus();
    }
}