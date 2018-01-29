package com.cimm2.writer;

import org.springframework.batch.item.ItemWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public abstract class AbstractWriter<T> implements ItemWriter<T> {

    protected PreparedStatement preparedStatement;
    protected Connection connection;
    private Long clientId;

    public void close() {
        try {
            if (preparedStatement != null)
                preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            preparedStatement = null;
        }

        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection = null;
        }
    }

    public void setClientId(Long clientId) {
        this.clientId = clientId;
    }

    public Long getClientId() {
        return clientId;
    }

    public abstract void write(List<? extends T> list) throws Exception;
}
