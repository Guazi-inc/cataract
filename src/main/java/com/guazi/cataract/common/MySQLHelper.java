package com.guazi.cataract.common;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.List;

public class MySQLHelper {
    private ComboPooledDataSource dataSource;
    private String driver = "com.mysql.jdbc.Driver";

    final Logger logger = LoggerFactory.getLogger(MySQLHelper.class);

    public MySQLHelper(String connectionString) throws ClassNotFoundException, SQLException {
        init(connectionString);
    }

    public void init(String connectionString) throws ClassNotFoundException, SQLException {
        logger.info("being to init connection");
        Class.forName(driver);
        String[] tks = connectionString.split(";");
        dataSource = new ComboPooledDataSource();
        dataSource.setUser(tks[4]);
        dataSource.setPassword(tks[5]);
        dataSource.setJdbcUrl(tks[0] + "://" + tks[1] + ":" + tks[2] + "/" + tks[3] + "?autoReconnect=true&characterEncoding=UTF-8");
        dataSource.setInitialPoolSize(1);
        dataSource.setMinPoolSize(1);
        dataSource.setMaxPoolSize(10);
        dataSource.setMaxStatements(50);
        dataSource.setMaxIdleTime(600);

        //http://www.databasesandlife.com/automatic-reconnect-from-hibernate-to-mysql/
        dataSource.setIdleConnectionTestPeriod(600);
        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setBreakAfterAcquireFailure(true);
        logger.info("init connection finished");
    }

    public Connection connect() throws SQLException {
        logger.info("begin connecting");
        Connection conn = dataSource.getConnection();
        logger.info("connecting finished");
        return conn;
    }

    public ResultSet read(Connection conn, String sql) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet ret = statement.executeQuery(sql);
        return ret;
    }

    public void close() throws SQLException {
        dataSource.close();
    }

    public boolean write(String sql, Object... objects) throws SQLException {
        Connection conn = dataSource.getConnection();
        try {
            conn.setAutoCommit(true); // Can't call commit when autocommit=true, so conn.commit(true) is commented
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            int n = 0;
            for (Object obj : objects) {
                ++n;
                if (obj instanceof String) {
                    preparedStatement.setString(n, (String) obj);
                } else if (obj instanceof Integer) {
                    preparedStatement.setInt(n, (Integer) obj);
                } else if (obj instanceof Date) {
                    preparedStatement.setDate(n, (Date) obj);
                } else if (obj instanceof Double) {
                    preparedStatement.setDouble(n, (Double) obj);
                } else {
                    return false;
                }
            }
            preparedStatement.executeUpdate();
            // conn.commit();
            return true;
        } catch (SQLException e) {
            logger.error("got SQLException:" + e.getMessage());
            return false;
        } finally {
            conn.close();
        }
    }

    public int[] writeBatch(String sql, List<List<Object>> lines) throws SQLException {
        Connection conn = dataSource.getConnection();
        try {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            for (List<Object> line : lines) {
                int n = 0;
                for (Object obj : line) {
                    ++n;
                    if (obj instanceof String) {
                        preparedStatement.setString(n, (String) obj);
                    } else if (obj instanceof Integer) {
                        preparedStatement.setInt(n, (Integer) obj);
                    } else if (obj instanceof Date) {
                        preparedStatement.setDate(n, (Date) obj);
                    } else if (obj instanceof Double) {
                        preparedStatement.setDouble(n, (Double) obj);
                    }
                }
                preparedStatement.addBatch();
            }
            return preparedStatement.executeBatch();
        } catch (SQLException e) {
            logger.error("got SQLException:" + e.getMessage());
        } finally {
            conn.close();
        }
        return null;
    }
}
