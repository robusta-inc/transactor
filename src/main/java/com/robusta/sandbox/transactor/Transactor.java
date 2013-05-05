package com.robusta.sandbox.transactor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface Transactor {
    void execute(String sql) throws TransactionException;
    <T> T queryForObject(String sql, PreparedStatementSetter preparedStatementSetter, RowMapper<T> rowMapper);
    <T> List<T> queryForList(String sql, PreparedStatementSetter preparedStatementSetter, RowMapper<T> rowMapper);
    int queryForInt(String sql, PreparedStatementSetter preparedStatementSetter);

    interface PreparedStatementCreator {
        PreparedStatement prepareStatement(Connection connection);
    }

    interface PreparedStatementCallback<T> {
        T doWithPreparedStatement(PreparedStatement preparedStatement) throws TransactionException, SQLException;
    }

    interface ConnectionCallback<T> {
        T doWithConnection(Connection connection) throws TransactionException, SQLException;
    }

    interface PreparedStatementSetter {
        void injectParametersInto(PreparedStatement preparedStatement) throws TransactionException, SQLException;
    }

    interface RowMapper<T> {
        T mapRow(ResultSet resultSet, int rowCount);
    }

    interface ResultSetCallback<T> {
        T doWithResultSet(ResultSet resultSet) throws SQLException;
    }
}
