package com.cimm2.job;

import com.cimm2.listener.JobListener;
import listener.ItemFailureLoggerListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class ItemExportJobConfiguration {


    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    protected JobListener jobListener;

    @Value("${export.execution.thread.count}")
    private Integer exportExecutionThreadCount;
    @Value("${spring.batch.chunk.size}")
    private Integer chunkSize;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private ItemFailureLoggerListener itemFailureLoggerListener;

    @Autowired
    @Qualifier("itemContentStep")
    private Step itemContentStep;

    @Autowired
    @Qualifier("excelFileWriterStep")
    private Step excelFileWriterStep;



    @Bean
    public Job itemExportJob() throws Exception {
        Flow itemExportFlow = new FlowBuilder<Flow>("itemExportFlow")
                .start(itemContentStep)
                .next(excelFileWriterStep)
                .end();

        return jobBuilderFactory.get("ItemExportJob")
                .listener(jobListener)
                .start(excelFileWriterStep)
                .build();
    }
}