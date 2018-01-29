package com.cimm2.util;

import org.apache.commons.lang.ArrayUtils;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public abstract class ModelUtil {

    public final static String clientCode = "test";

    @Autowired
    protected DSLContext dslContext;

    @Autowired
    @Qualifier("itemHeader")
    protected String[] header;

    public static int getCellIndex(String columnName, String[] header) {
        int i = ArrayUtils.indexOf(header, columnName);
        return i;
    }

    public static String getCellValue(String columnName, String[] row, String[] header) {
        int cellIndex = getCellIndex(columnName, header);
        if (cellIndex != -1 && cellIndex < row.length) {
            return row[cellIndex];
        }
        return null;
    }

    public int getCellIndex(String columnName) {
        int i = ArrayUtils.indexOf(header, columnName);
        return i;
    }

    public String getCellValue(String columnName, String[] row) {
        int cellIndex = getCellIndex(columnName);
        if (cellIndex != -1 && cellIndex < row.length) {
            return row[cellIndex];
        }
        return null;
    }
}
