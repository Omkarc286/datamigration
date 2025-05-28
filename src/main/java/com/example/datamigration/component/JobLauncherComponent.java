package com.example.datamigration.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class JobLauncherComponent {

    private static final Logger logger = LoggerFactory.getLogger(JobLauncherComponent.class);

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job importJob;  // This must match the exact bean name from your BatchConfig

    @EventListener(ApplicationReadyEvent.class)
    public void launchJobAfterStartup() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addDate("launchDate", new Date())
                    .toJobParameters();

            logger.info("Starting importJob with parameters: {}", params);
            jobLauncher.run(importJob, params);
        } catch (Exception e) {
            logger.error("Error launching job: {}", e.getMessage(), e);
        }
    }
}