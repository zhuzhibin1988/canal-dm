package com.eshore.otter.canal.parse.driver.dameng;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 执行sql（包括start_logmnr,add_logfile,end_logmnr,查询归档日志）
 *
 * @author zhuzhibin
 * @since 1.0.0
 */
public class DamengQueryExecutor {

    private DamengConnector connector;

    public DamengQueryExecutor(DamengConnector connector) throws SQLException {
        if (!connector.isConnected()) {
            throw new SQLException("should execute connector.connect() first");
        }
        this.connector = connector;
    }

    public ResultSet query(String sql) throws SQLException {
        ResultSet rs = this.connector.connect().createStatement().executeQuery(sql);
        return rs;
    }

    public List query(String sql, ResultSetProcessor resultSetProcessor) throws SQLException {
        ResultSet rs = this.connector.connect().createStatement().executeQuery(sql);
        return resultSetProcessor.process(rs);
    }

    public void execute(String sql) throws SQLException {
        this.connector.connect().createStatement().execute(sql);
    }

    @FunctionalInterface
    public interface ResultSetProcessor<T> {
        List<T> process(ResultSet rs);
    }
}
