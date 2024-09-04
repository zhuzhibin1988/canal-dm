package com.eshore.otter.canal.parse.driver.dameng;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 执行sql（包括start_logmnr,add_logfile,end_logmnr,查询归档日志）
 *
 * @author zhuzhibin
 * @since 1.0.0
 */
public class DamengUpdateExecutor {

    private DamengConnector connector;

    public DamengUpdateExecutor(DamengConnector connector) throws SQLException {
        if (!connector.isConnected()) {
            throw new SQLException("should execute connector.connect() first");
        }
        this.connector = connector;
    }

    public int update(String sql, List<Object> objects, StatementProcessor statementProcessor) throws SQLException {
        PreparedStatement ps = this.connector.connect().prepareStatement(sql);
        statementProcessor.accept(ps, objects);
        return ps.executeUpdate();
    }

    public int update(String sql) throws SQLException {
        PreparedStatement ps = this.connector.connect().prepareStatement(sql);
        return ps.executeUpdate();
    }

    @FunctionalInterface
    public interface StatementProcessor {
        void accept(PreparedStatement ps, List<Object> objects);
    }
}
