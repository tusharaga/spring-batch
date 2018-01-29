package com.cimm2.mapper;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StringToStringArrayRowMapper implements RowMapper<String[]> {

    private String[] header;

    public StringToStringArrayRowMapper(String[] header) {
        this.header = header;
    }

    @Override
    public String[] mapRow(ResultSet rs, int rowNum) throws SQLException {
        String[] cells = new String[header.length];

        for (int i = 0; i < header.length; i++) {
            cells[i] = rs.getString(header[i]);
        }
        return cells;
    }

}
