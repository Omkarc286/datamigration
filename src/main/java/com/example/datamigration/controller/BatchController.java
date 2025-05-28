package com.example.datamigration.controller;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.launch.JobOperator;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;
import org.springframework.http.HttpStatus;

@RestController
@RequestMapping("/api/batch")
public class BatchController {

    private final JobExplorer jobExplorer;
    private final JobLauncher jobLauncher;
    private final JobOperator jobOperator;
    private final JobRegistry jobRegistry;
    private final Job importJob;

    @Autowired // Only need this once on the constructor
    public BatchController(JobExplorer jobExplorer, JobOperator jobOperator,
                           JobRegistry jobRegistry, JobLauncher jobLauncher,
                           Job importJob) {
        this.jobExplorer = jobExplorer;
        this.jobOperator = jobOperator;
        this.jobRegistry = jobRegistry;
        this.jobLauncher = jobLauncher;
        this.importJob = importJob;
    }
    @GetMapping("/status")
    public Map<String, Object> getBatchStatus() {
        Map<String, Object> result = new HashMap<>();
        try {
            List<String> jobNames = jobExplorer.getJobNames();

            if (jobNames.isEmpty()) {
                result.put("status", "No jobs found");
                return result;
            }

            String jobName = "importJob"; // The job name from your BatchConfig
            JobInstance lastJobInstance = jobExplorer.getLastJobInstance(jobName);

            if (lastJobInstance == null) {
                result.put("status", "Job not yet executed");
                return result;
            }

            List<JobExecution> executions = jobExplorer.getJobExecutions(lastJobInstance);
            if (executions.isEmpty()) {
                result.put("status", "No executions found");
                return result;
            }

            JobExecution lastExecution = executions.get(0);
            result.put("jobName", jobName);
            result.put("jobStatus", lastExecution.getStatus().toString());
            result.put("startTime", lastExecution.getStartTime());
            result.put("endTime", lastExecution.getEndTime());

            Map<String, Object> stepStats = new HashMap<>();
            for (StepExecution stepExecution : lastExecution.getStepExecutions()) {
                Map<String, Object> stats = new HashMap<>();
                stats.put("status", stepExecution.getStatus().toString());
                stats.put("readCount", stepExecution.getReadCount());
                stats.put("writeCount", stepExecution.getWriteCount());
                stats.put("processSkipCount", stepExecution.getProcessSkipCount());
                stats.put("commitCount", stepExecution.getCommitCount());
                stats.put("rollbackCount", stepExecution.getRollbackCount());

                if (stepExecution.getReadCount() > 0) {
                    double progress = (double) stepExecution.getWriteCount() / stepExecution.getReadCount() * 100;
                    stats.put("progress", String.format("%.2f%%", progress));
                }

                stepStats.put(stepExecution.getStepName(), stats);
            }

            result.put("steps", stepStats);
        } catch (Exception e) {
            result.put("status", "Error: Batch tables not initialized yet");
            result.put("message", "Please try again after job starts");
            result.put("error", e.getMessage());
        }

        return result;
    }
    @PostMapping("/start")
    public ResponseEntity<String> startBatchJob() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addDate("launchDate", new Date())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(importJob, params);
            return ResponseEntity.ok("Job started with status: " + execution.getStatus());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error starting job: " + e.getMessage());
        }
    }
    @PostMapping("/stop")
    public ResponseEntity<String> stopJob() {
        try {
            Set<JobExecution> runningJobs = findRunningJobs();
            if (runningJobs.isEmpty()) {
                return ResponseEntity.ok("No running jobs found");
            }

            for (JobExecution jobExecution : runningJobs) {
                jobOperator.stop(jobExecution.getId());
            }

            return ResponseEntity.ok("Stop signal sent to " + runningJobs.size() + " running jobs");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error stopping job: " + e.getMessage());
        }
    }

    @PostMapping("/restart")
    public ResponseEntity<String> restartJob() {
        try {
            Set<JobExecution> failedJobs = findFailedJobs();
            if (failedJobs.isEmpty()) {
                return ResponseEntity.ok("No failed jobs found to restart");
            }

            Long restartedJobId = null;
            for (JobExecution jobExecution : failedJobs) {
                restartedJobId = jobOperator.restart(jobExecution.getId());
                break; // Just restart the most recent failed job
            }

            return ResponseEntity.ok("Job restarted with execution ID: " + restartedJobId);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error restarting job: " + e.getMessage());
        }
    }

    // Utility method to find running jobs
    private Set<JobExecution> findRunningJobs() {
        Set<JobExecution> runningExecutions = new HashSet<>();

        for (String jobName : jobExplorer.getJobNames()) {
            List<JobInstance> instances = jobExplorer.findJobInstancesByJobName(jobName, 0, 100);
            for (JobInstance instance : instances) {
                List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
                for (JobExecution execution : executions) {
                    if (execution.isRunning()) {
                        runningExecutions.add(execution);
                    }
                }
            }
        }

        return runningExecutions;
    }

    // Utility method to find failed jobs
    private Set<JobExecution> findFailedJobs() {
        Set<JobExecution> failedExecutions = new HashSet<>();

        for (String jobName : jobExplorer.getJobNames()) {
            List<JobInstance> instances = jobExplorer.findJobInstancesByJobName(jobName, 0, 100);
            for (JobInstance instance : instances) {
                List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
                for (JobExecution execution : executions) {
                    if (execution.getStatus() == BatchStatus.FAILED) {
                        failedExecutions.add(execution);
                    }
                }
            }
        }

        return failedExecutions;
    }

    // Force a job to transition to a FAILED state
    @PostMapping("/force-fail")
    public ResponseEntity<String> forceFailStuckJob() {
        try {
            Set<JobExecution> stuckJobs = findStuckJobs();
            if (stuckJobs.isEmpty()) {
                return ResponseEntity.ok("No stuck jobs found");
            }

            int count = 0;
            for (JobExecution jobExecution : stuckJobs) {
                jobOperator.stop(jobExecution.getId());
                count++;
            }

            return ResponseEntity.ok("Force-failed " + count + " stuck jobs");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error force-failing job: " + e.getMessage());
        }
    }

    // Find jobs that are stuck (started a long time ago and still running)
    private Set<JobExecution> findStuckJobs() {
        Set<JobExecution> stuckExecutions = new HashSet<>();
        LocalDateTime twoHoursAgo = LocalDateTime.now().minusHours(2);

        for (String jobName : jobExplorer.getJobNames()) {
            List<JobInstance> instances = jobExplorer.findJobInstancesByJobName(jobName, 0, 100);
            for (JobInstance instance : instances) {
                List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
                for (JobExecution execution : executions) {
                    if (execution.isRunning() &&
                            execution.getStartTime().isBefore(twoHoursAgo)) {
                        stuckExecutions.add(execution);
                    }
                }
            }
        }

        return stuckExecutions;
    }
}