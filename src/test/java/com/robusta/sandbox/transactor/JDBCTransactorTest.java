package com.robusta.sandbox.transactor;

import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.robusta.sandbox.transactor.JDBCTransactorTest.DataSourceMocker.aDataSource;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class JDBCTransactorTest {
    private JDBCTransactor transactor;
    private DataSourceMocker dataSourceMocker = aDataSource();

    @Before
    public void setUp() throws Exception {
        transactor = new JDBCTransactor(dataSourceMocker.serveConnection().mock());
    }

    @Test
    public void testExecute() throws Exception {

    }

    @Test
    public void testQueryForObject() throws Exception {
        String sql = "select count(*) from TABLE_A";
        dataSourceMocker.preparedStatement(sql).queryForResultSet().withRowCount(1).withRowData(1, 100);
        assertThat(transactor.queryForInt(sql, transactor.NO_OP_PREPARED_STATEMENT_SETTER), is(100));
        dataSourceMocker.verify();
    }

    @Test
    public void testQueryForList() throws Exception {

    }

    @Test
    public void testQueryForInt() throws Exception {
        String sql = "select count(*) from TABLE_A";
        dataSourceMocker.preparedStatement(sql).queryForResultSet().withRowCount(1).withRowData(1, 100);
        assertThat(transactor.queryForInt(sql, transactor.NO_OP_PREPARED_STATEMENT_SETTER), is(100));
        dataSourceMocker.verify();
    }

    @Test(expected = TransactionException.class)
    public void testQueryForInt_whenResultSetReturnsNoRows_shouldTranslateAndThrowException() throws Exception {
        String sql = "select count(*) from TABLE_A";
        dataSourceMocker.preparedStatement(sql).queryForResultSet().hasNoRows();
        assertThat(transactor.queryForInt(sql, transactor.NO_OP_PREPARED_STATEMENT_SETTER), is(100));
    }

    public static class DataSourceMocker {
        private DataSource dataSource;
        private Connection connection;
        private PreparedStatement preparedStatement;
        private ResultSet resultSet;
        private Optional<Integer> rowCount;

        private DataSourceMocker() {
            dataSource =  Mockito.mock(DataSource.class);
            connection =  Mockito.mock(Connection.class);
            preparedStatement =  Mockito.mock(PreparedStatement.class);
            resultSet =  Mockito.mock(ResultSet.class);
        }

        public static DataSourceMocker aDataSource() {
            return new DataSourceMocker();
        }

        public DataSourceMocker serveConnection() {
            return this;
        }

        public DataSourceMocker preparedStatement(String sql) throws SQLException {
            when(connection.prepareStatement(sql)).thenReturn(preparedStatement); return this;
        }

        public DataSourceMocker queryForResultSet() throws SQLException {
            when(preparedStatement.executeQuery()).thenReturn(resultSet); return this;
        }

        public DataSource mock() throws SQLException {
            when(dataSource.getConnection()).thenReturn(connection);
            return dataSource;
        }

        public DataSourceMocker withRowCount(int rowCount) throws SQLException {
            when(resultSet.next()).thenReturn(true);
            this.rowCount = Optional.of(rowCount);
            return this;
        }

        public DataSourceMocker verify() throws SQLException {
            if(rowCount.isPresent()) {
                Mockito.verify(resultSet, times(rowCount.get())).next();
            }
            return this;
        }

        public DataSourceMocker withRowData(int column, int data) throws SQLException {
            when(resultSet.getInt(column)).thenReturn(data); return this;
        }

        public DataSourceMocker hasNoRows() throws SQLException {
            when(resultSet.next()).thenReturn(false); return this;
        }
    }
}
