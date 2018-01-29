package com.cimm2.step;


import com.cimm2.mapper.StringToStringArrayRowMapper;
import com.cimm2.writer.ExcelFileItemWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ExcelFileItemWriterStep extends AbstractStep {

    @Value("${export.staging.item.select.clause}")
    private String stagingItemSelectClause;


    @Value("${export.data.folder}")
    private String exportDataFolder;

    @Value("${export.staging.item.table}")
    private String exportStagingItemTable;


    public String[] stagingItemHeader() {
        return createHeader(stagingItemSelectClause);
    }

    @Bean
    public StringToStringArrayRowMapper excelFileItemRowMapper() {
        return new StringToStringArrayRowMapper(stagingItemHeader());
    }


    @Bean
    @StepScope
    public ItemReader<String[]> excelFileItemReader(
            @Value("#{stepExecutionContext['minValue']}") Long minValue,
            @Value("#{stepExecutionContext['maxValue']}") Long maxValue,
            @Value("#{jobParameters}") Map<String, Object> jobParameters) {
        String clientId = (String) jobParameters.get("{clientId");
        System.out.println("Reading staging item records from range " + minValue + " to " + maxValue + " for client-id=" + clientId);
        JdbcPagingItemReader<String[]> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("ID", Order.ASCENDING);

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause(stagingItemSelectClause);
        queryProvider.setFromClause(exportStagingItemTable);
        queryProvider.setSortKeys(sortKeys);
        queryProvider.setWhereClause("where ID >= " + minValue + " and ID <= " + maxValue + " and CLIENT_ID = " + clientId);

        reader.setQueryProvider(queryProvider);
        reader.setRowMapper(excelFileItemRowMapper());
        return reader;
    }


    @Bean(destroyMethod = "close")
    @StepScope
    public ItemWriter excelFileItemWriter(@Value("#{jobParameters}") Map<String, Object> jobParameters) {
        String clientId = (String) jobParameters.get("{clientId");
        String exportType = (String) jobParameters.get("exportType");
        String columns = (String) jobParameters.get("columns");
        Validate.isTrue(StringUtils.isNotBlank(clientId), "Please start job by providing clientCode");
        String file = exportDataFolder + File.separator + "item/"+Thread.currentThread().getId()+".xlsx";
        return new ExcelFileItemWriter(file, stagingItemHeader());
    }


    @Bean
    @SuppressWarnings("unchecked")
    public Step excelFileWriterSlaveStep() {
        return stepBuilderFactory.get("excelFileWriterSlaveStep")
                .<String[], String[]>chunk(100)
                .listener(stepListener)
                .faultTolerant()
                .listener(chunkListener)
                .reader(excelFileItemReader(null, null, null))
                .writer(excelFileItemWriter(null))
                .build();
    }


    @Bean
    public Step excelFileWriterStep() throws Exception {
        return stepBuilderFactory.get("excelFileWriterStep")
                .partitioner(excelFileWriterSlaveStep().getName(), columnRangePartitioner(exportStagingItemTable))
                .step(excelFileWriterSlaveStep())
                .gridSize(exportExecutionThreadCount)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }
}

