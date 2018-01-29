package com.cimm2.job;

import com.cimm2.listener.ChunkListener;
import com.cimm2.listener.JobListener;
import com.cimm2.listener.StepListener;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import listener.ItemFailureLoggerListener;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.NoWorkFoundStepExecutionListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

@Configuration
public class BaseJobConfiguration extends DefaultBatchConfigurer implements ApplicationContextAware {


    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobExplorer jobExplorer;

    @Autowired
    public JobRepository jobRepository;

    @Autowired
    public JobRegistry jobRegistry;

    @Autowired
    public JobLauncher jobLauncher;

    private ApplicationContext applicationContext;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistrar() throws Exception {
        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
        registrar.setJobRegistry(this.jobRegistry);
        registrar.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
        registrar.afterPropertiesSet();
        return registrar;
    }

    @Bean
    public JobOperator jobOperator() throws Exception {
        SimpleJobOperator simpleJobOperator = new SimpleJobOperator();
        simpleJobOperator.setJobLauncher(this.jobLauncher);
        simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
        simpleJobOperator.setJobRepository(this.jobRepository);
        simpleJobOperator.setJobExplorer(this.jobExplorer);
        simpleJobOperator.setJobRegistry(this.jobRegistry);
        simpleJobOperator.afterPropertiesSet();
        return simpleJobOperator;
    }

    @Override
    public JobLauncher getJobLauncher() {
        SimpleJobLauncher jobLauncher = null;
        try {
            jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(jobRepository);
            jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
            jobLauncher.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobLauncher;
    }

    @Bean
    public ChunkListener chunkListener() {
        return new ChunkListener();
    }

    @Bean
    public ItemFailureLoggerListener itemFailureLoggerListener() {
        return new ItemFailureLoggerListener();
    }

    @Bean
    public DSLContext dslContext() {
        DSLContext dslContext = DSL.using(dataSource(), SQLDialect.MYSQL);
        return dslContext;
    }


    @Bean(destroyMethod = "close")
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig("/datasource.properties");
        HikariDataSource dataSource = new HikariDataSource(config);

        return dataSource;
    }

    @Bean
    public JobListener jobListener() {
        return new JobListener();
    }

    @Bean
    @StepScope
    public StepListener stepListener() {
        return new StepListener();
    }


    @Bean
    protected ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("task-executor-thread-");
        executor.setCorePoolSize(4);
        return executor;
    }

    @Bean
    public NoWorkFoundStepExecutionListener noWorkFoundStepExecutionListener() {
        return new NoWorkFoundStepExecutionListener();
    }


}
