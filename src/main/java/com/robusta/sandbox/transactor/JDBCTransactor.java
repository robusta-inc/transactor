package com.robusta.sandbox.transactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;

public class JDBCTransactor implements Transactor {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    public final PreparedStatementSetter NO_OP_PREPARED_STATEMENT_SETTER = new NoOpPreparedStatementSetter();

    private final DataSource dataSource;

    public JDBCTransactor(DataSource dataSource) {
        checkArgument(dataSource != null, "A valid not null data source is required for JDBCTransactor to initialize");
        this.dataSource = dataSource;
    }

    protected Connection borrowConnectionFromDataSource() {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            LOGGER.trace("Borrowed a connection from the data source, set auto commit to FALSE, returning connection");
            return connection;
        } catch (SQLException e) {
            LOGGER.trace("JDBCTransactor was unable to borrow a connection from the data source");
            throw new TransactionException("JDBCTransactor was unable to borrow a connection from the data source", e);
        }
    }

    @Override
    public void execute(String sql) throws TransactionException {
        execute(new DefaultPreparedStatementCreator(sql),
                new DefaultPreparedStatementCallback<Object>(
                        new NoOpPreparedStatementSetter(),
                        new NoOpResultSetCallback()));
    }

    @Override
    public <T> T queryForObject(String sql, final PreparedStatementSetter preparedStatementSetter, final RowMapper<T> rowMapper) {
        return execute(
                new DefaultPreparedStatementCreator(sql),
                new DefaultPreparedStatementCallback<T>(
                        preparedStatementSetter,
                        new SingleResultRowMappingResultSetCallback<T>(
                                new RowMappingResultSetCallback<T>(
                                        rowMapper))));
    }

    @Override
    public <T> List<T> queryForList(String sql, PreparedStatementSetter preparedStatementSetter, RowMapper<T> rowMapper) {
        return execute(new DefaultPreparedStatementCreator(sql),
                new DefaultPreparedStatementCallback<List<T>>(
                        preparedStatementSetter,
                        new RowMappingResultSetCallback<T>(rowMapper)));
    }

    @Override
    public int queryForInt(String sql, PreparedStatementSetter preparedStatementSetter) {
        return execute(new DefaultPreparedStatementCreator(sql),
                new DefaultPreparedStatementCallback<Integer>(
                        preparedStatementSetter,
                        new IntegerFetchingResultSetCallback()));
    }

    public class SingleResultRowMappingResultSetCallback<T> implements ResultSetCallback<T> {

        private final ResultSetCallback<List<T>> resultSetCallback;

        public SingleResultRowMappingResultSetCallback(ResultSetCallback<List<T>> resultSetCallback) {
            this.resultSetCallback = resultSetCallback;
        }


        @Override
        public T doWithResultSet(ResultSet resultSet) throws SQLException {
            List<T> resultList = resultSetCallback.doWithResultSet(resultSet);
            if (resultList.size() == 1) {
                return resultList.get(0);
            }
            throw new TransactionException(String.format("Result set callback did not result in a single result: '%s'", resultList.size()));
        }
    }

    public class RowMappingResultSetCallback<T> implements ResultSetCallback<List<T>> {
        private final RowMapper<T> rowMapper;

        public RowMappingResultSetCallback(RowMapper<T> rowMapper) {
            this.rowMapper = rowMapper;
        }

        @Override
        public List<T> doWithResultSet(ResultSet resultSet) throws SQLException {
            List<T> resultsList = newArrayList();
            int rowCount = 0;
            while (resultSet.next()) {
                resultsList.add(rowMapper.mapRow(resultSet, rowCount++));
            }
            return resultsList;
        }
    }

    public class DefaultPreparedStatementCallback<T> implements PreparedStatementCallback<T> {
        private final PreparedStatementSetter preparedStatementSetter;
        private final ResultSetCallback<T> resultSetCallback;

        public DefaultPreparedStatementCallback(PreparedStatementSetter preparedStatementSetter, ResultSetCallback<T> resultSetCallback) {
            this.preparedStatementSetter = preparedStatementSetter;
            this.resultSetCallback = resultSetCallback;
        }


        @Override
        public T doWithPreparedStatement(PreparedStatement preparedStatement) throws TransactionException, SQLException {
            preparedStatementSetter.injectParametersInto(preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            try {
                return resultSetCallback.doWithResultSet(resultSet);
            } finally {
                closeThisResultSet(resultSet);
            }
        }

        protected void closeThisResultSet(ResultSet resultSet) {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.warn("SQL Exception trying to close the result set, logging and ignoring", e);
                }
            }
        }
    }

    protected <T> T execute(ConnectionCallback<T> connectionCallback) throws TransactionException {
        checkArgument(connectionCallback != null, "A valid connection call back is required.");
        Connection connection = borrowConnectionFromDataSource();
        LOGGER.trace("Borrowed a connection, passing connection to the connection callback");
        try {
            T whatTheCallBackReturned = connectionCallback.doWithConnection(connection);
            LOGGER.trace("Connection callback finished with connection");
            commitThisTransaction(connection);
            return whatTheCallBackReturned;
        } catch (TransactionException e) {
            LOGGER.trace("TransactionException from Connection Callback, rolling back the transaction before re-throwing exception");
            rollbackThisTransaction(connection, e);
            throw e;
        } catch (Exception e) {
            LOGGER.trace("Generic Exception from Connection Callback, rolling back the transaction before re-throwing exception");
            rollbackThisTransaction(connection, e);
            throw new TransactionException("Generic Exception from Connection Callback", e);
        } finally {
            closeAndReturnTheConnection(connection);
        }
    }

    private void closeAndReturnTheConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.warn("SQL Exception trying to close the connection, logging and ignoring", e);
            }
        }
    }

    private void rollbackThisTransaction(Connection connection, Exception e) {
        try {
            connection.rollback();
        } catch (SQLException e1) {
            LOGGER.error("Rolling back the transaction failed with SQL exception. Reason for rollback was: ", e);
            throw new TransactionException(String.format("Rollback of transaction due to: '%s' has failed", e.getMessage()), e);
        }
    }

    private void commitThisTransaction(Connection connection) {
        try {
            connection.commit();
        } catch (SQLException e) {
            LOGGER.trace("Unable to commit the transaction after completion of connection callbacks");
            throw new TransactionException("Unable to commit the transaction after completion of connection callbacks", e);
        }
    }

    protected <T> T execute(final PreparedStatementCreator preparedStatementCreator, final PreparedStatementCallback<T> preparedStatementCallback) throws TransactionException {
        return execute(new DefaultConnectionCallback<T>(preparedStatementCreator, preparedStatementCallback));
    }

    private class NoOpPreparedStatementSetter implements PreparedStatementSetter {
        @Override
        public void injectParametersInto(PreparedStatement preparedStatement) throws TransactionException, SQLException {

        }
    }

    private class NoOpResultSetCallback implements ResultSetCallback<Object> {
        @Override
        public Object doWithResultSet(ResultSet resultSet) throws SQLException {
            return null;
        }
    }

    private class DefaultConnectionCallback<T> implements ConnectionCallback<T> {
        private final PreparedStatementCreator preparedStatementCreator;
        private final PreparedStatementCallback<T> preparedStatementCallback;

        public DefaultConnectionCallback(PreparedStatementCreator preparedStatementCreator, PreparedStatementCallback<T> preparedStatementCallback) {
            this.preparedStatementCreator = preparedStatementCreator;
            this.preparedStatementCallback = preparedStatementCallback;
        }

        @Override
        public T doWithConnection(Connection connection) throws TransactionException, SQLException {
            PreparedStatement preparedStatement = preparedStatementCreator.prepareStatement(connection);
            try {
                return preparedStatementCallback.doWithPreparedStatement(preparedStatement);
            } finally {
                finallyCloseThePreparedStatement(preparedStatement);
            }

        }

        protected void finallyCloseThePreparedStatement(PreparedStatement preparedStatement) {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    LOGGER.warn("SQL Exception trying to close the prepared statement, logging and ignoring", e);
                }
            }
        }
    }

    public class DefaultPreparedStatementCreator implements PreparedStatementCreator {
        private final String sql;

        public DefaultPreparedStatementCreator(String sql) {
            checkArgument(!isNullOrEmpty(sql), "A valid SQL (as string) is required to prepare the statement");
            this.sql = sql;
        }

        @Override
        public PreparedStatement prepareStatement(Connection connection) {
            checkArgument(connection != null, "A valid Connection is required to prepare the statement");
            LOGGER.trace("Asked to prepare a statement for SQL string: '{}'", sql);
            try {
                //noinspection ConstantConditions
                return connection.prepareStatement(sql);
            } catch (SQLException e) {
                LOGGER.trace("An unexpected error occurred during preparing the statement");
                throw new TransactionException("An unexpected error occurred during preparing the statement", e);
            }
        }
    }

    private class IntegerFetchingResultSetCallback implements ResultSetCallback<Integer> {
        @Override
        public Integer doWithResultSet(ResultSet resultSet) throws SQLException {
            if(resultSet.next()) {
                return resultSet.getInt(1);
            }
            throw new TransactionException("A result set with a count was expected, but result set did not return any rows");
        }
    }
}