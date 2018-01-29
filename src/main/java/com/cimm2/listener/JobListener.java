package com.cimm2.listener;

import com.cimm2.util.CommonUtil;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.util.Date;

public class JobListener implements JobExecutionListener {


    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Before Job Execution Listener, Thread:" + Thread.currentThread().getName() + " Object hashcode:" + this);
    }


    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("After Job Execution Listener Thread:" + Thread.currentThread().getName() + " Object hashcode:" + this);
        Date startTime = jobExecution.getStartTime();
        Date endTime = new Date();
        String jobName = jobExecution.getJobInstance().getJobName();

        System.out.println("--Job-- " + jobName + " took " + CommonUtil.getTimeDiff(endTime, startTime));
    }
}