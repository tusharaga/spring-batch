package com.cimm2.step.staging.item;

import com.cimm2.mapper.staging.ItemContentRowMapper;
import com.cimm2.step.AbstractStep;
import com.cimm2.writer.staging.item.ItemContentItemWriter;
import com.tushar.cimm2.db.model.Tables;
import com.tushar.cimm2.db.model.tables.records.ExportStagingItemRecord;
import com.tushar.cimm2.db.model.tables.records.TmbBrandRecord;
import com.tushar.cimm2.db.model.tables.records.TmbManufacturerRecord;
import org.apache.commons.lang.Validate;
import org.jooq.types.ULong;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ItemContentStepWriterStep  extends AbstractStep {

    @Value("${pim.item.table}")
    private String itemTable;

    @Value("${pim.item.columns}")
    private String itemColumns;


    @Bean
    public ItemContentRowMapper itemRowMapper() {
        Map<ULong, TmbBrandRecord> brandRecordMap= dslContext
                .selectFrom(Tables.TMB_BRAND).fetch().intoMap(Tables.TMB_BRAND.ID);

        Map<ULong, TmbManufacturerRecord> manufacturerRecordMap = dslContext
                .selectFrom(Tables.TMB_MANUFACTURER).fetch().intoMap(Tables.TMB_MANUFACTURER.ID);
        ItemContentRowMapper rowMapper = new ItemContentRowMapper(manufacturerRecordMap, brandRecordMap);
        return rowMapper;
    }

    @Bean("itemHeader")
    public String[] itemHeader() {
        return createHeader(itemColumns);
    }

    @Bean
    @StepScope
    public ItemReader<ExportStagingItemRecord> itemItemReader(
            @Value("#{stepExecutionContext['minValue']}") Long minValue,
            @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        System.out.println("Reading item records from range " + minValue + " to " + maxValue);
        JdbcPagingItemReader<ExportStagingItemRecord> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("ID", Order.ASCENDING);

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause(itemColumns);
        queryProvider.setFromClause(" FROM "+ itemTable);
        queryProvider.setSortKeys(sortKeys);
        queryProvider.setWhereClause("where ID >= " + minValue + " and ID < " + maxValue);

        reader.setQueryProvider(queryProvider);
        reader.setRowMapper(itemRowMapper());

        return reader;
    }


    @Bean(destroyMethod = "close")
    @StepScope
    public ItemWriter itemItemWriter(@Value("#{jobParameters}") Map<String, Object> jobParameters) {
        return new ItemContentItemWriter();
    }


    @Bean
    @SuppressWarnings("unchecked")
    public Step itemContentSlaveStep() {
        return stepBuilderFactory.get("itemItemSlaveStep")
                .<String[], String[]>chunk(100)
                .listener(stepListener)
                .faultTolerant()
                .listener(chunkListener)
                .reader(itemItemReader(null, null))
                .writer(itemItemWriter(null))
                .build();
    }


    @Bean
    public Step itemContentStep() throws Exception {
        return stepBuilderFactory.get("itemContentStep")
                .partitioner(itemContentSlaveStep().getName(), columnRangePartitioner(itemTable))
                .step(itemContentSlaveStep())
//                .gridSize(importExecutionThreadCount)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }
}


