package com.cimm2.step;


import com.cimm2.listener.ChunkListener;
import com.cimm2.listener.StepListener;
import com.cimm2.partitioner.ColumnRangePartitioner;
import org.jooq.DSLContext;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.NoWorkFoundStepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.util.Map;

public abstract class AbstractStep {


    @Autowired
    protected DSLContext dslContext;
    @Autowired
    protected DataSource dataSource;
    @Autowired
    protected StepBuilderFactory stepBuilderFactory;
    @Autowired
    protected StepListener stepListener;
    @Autowired
    protected ChunkListener chunkListener;
    @Autowired
    protected NoWorkFoundStepExecutionListener noWorkFoundStepExecutionListener;

    @Value("${export.execution.thread.count}")
    protected Integer exportExecutionThreadCount;

    @Bean
    public ColumnRangePartitioner columnRangePartitioner(String tableName) {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
        columnRangePartitioner.setColumn("ID");
        columnRangePartitioner.setDataSource(dataSource);
        columnRangePartitioner.setTable(tableName);
        columnRangePartitioner.setClientId("1");
        return columnRangePartitioner;
    }


    public String[] createHeader(String selectClause) {
        String quotesRemoved = selectClause;
        String[] columnsWithSpaces = quotesRemoved.split(",");
        String[] trimmed = new String[columnsWithSpaces.length];

        for (int i = 0; i < columnsWithSpaces.length; i++) {
            trimmed[i] = columnsWithSpaces[i].trim();
        }
        return trimmed;
    }
}
