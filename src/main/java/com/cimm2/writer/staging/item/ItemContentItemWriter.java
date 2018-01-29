package com.cimm2.writer.staging.item;

import com.cimm2.writer.AbstractWriter;
import com.tushar.cimm2.db.model.tables.records.ExportStagingItemRecord;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.util.List;

public class ItemContentItemWriter extends AbstractWriter<ExportStagingItemRecord> {

    @Autowired
    private DSLContext dslContext;

    @Autowired
    DataSource dataSource;

    @Override
    public void write(List<? extends ExportStagingItemRecord> list) throws Exception {
        try {
            dslContext.batchInsert(list).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
