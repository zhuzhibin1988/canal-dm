package com.eshore.dm.cdc.util;

import com.eshore.dm.cdc.bean.ArchiveFile;
import com.eshore.dm.cdc.bean.LogEntry;
import lombok.Builder;
import lombok.Data;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 日志挖掘工具
 *
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/30 14:20
 * @Description: TODO
 */
public class LogMnr {
    private Connection connection;

    public LogMnr(Connection connection) {
        this.connection = connection;
    }

    /**
     * 获取当前活动归档文件
     *
     * @return
     */
    public ArchiveFile getActiveArchiveFile() {
        ArchiveFile archiveFile = null;
        Statement statement = null;
        try {
            String sql = "select path,status,arch_lsn from V$ARCH_FILE where status = 'ACTIVE'";
            statement = this.connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                archiveFile = ArchiveFile.builder()
                        .archLSN(rs.getLong("arch_lsn"))
                        .path(rs.getString("path"))
                        .status(rs.getString("status"))
                        .build();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return archiveFile;
    }

    /**
     * 获取指定归档文件元数据
     *
     * @param archLSN
     * @return
     */
    public ArchiveFile getNextArchiveFile(long archLSN) {
        ArchiveFile archiveFile = null;
        PreparedStatement ps = null;
        try {
            String sql = "select path,status,arch_lsn from V$ARCH_FILE where arch_lsn > ? order by arch_lsn";
            ps = this.connection.prepareStatement(sql);
            ps.setFetchSize(1);
            ps.setLong(1, archLSN);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                archiveFile = ArchiveFile.builder()
                        .archLSN(rs.getLong("arch_lsn"))
                        .path(rs.getString("path"))
                        .status(rs.getString("status"))
                        .build();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return archiveFile;
    }

    public String getArchiveFileStatus(long archLSN) {
        PreparedStatement ps = null;
        try {
            String sql = "select status from V$ARCH_FILE where arch_lsn = ? ";
            ps = this.connection.prepareStatement(sql);
            ps.setFetchSize(1);
            ps.setLong(1, archLSN);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString("status");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * 添加归档日志文件
     * 该方法用于将指定路径的归档日志文件添加到数据库的日志挖掘中
     * <pre>Options (NUMBER)</pre>
     * <pre>描述：指定如何处理传入的日志文件：</pre>
     * <pre>DBMS_lOGMNR."NEW": 结束当前 LOGMNR（调用 LOGMNR_END），并增加指定文件（如果已经 START，则不可增加）</pre>
     * <pre>DBMS_lOGMNR.ADDFILE : 在当前 LOGMNR 中增加日志文件（如果已经 START，则不可增加）</pre>
     * <pre>DBMS_LOGMNR.REMOVEFILE: 从当前 LOGMNR 中去除一个日志文件</pre>
     *
     * @param archiveFilePath 归档日志文件的路径
     * @throws SQLException 如果执行过程中发生数据库访问错误
     */
    public void addArchiveLogFile(String archiveFilePath) throws SQLException {
        String sql = "call DBMS_LOGMNR.ADD_LOGFILE(logfilename=>?, options=>DBMS_LOGMNR.\"NEW\")";
        CallableStatement cs = this.connection.prepareCall(sql);
        cs.setString(1, archiveFilePath);
        cs.execute();
        cs.close();
    }

    public void startLogMining(Long startScn, Long endScn) throws SQLException {
        String sql = "call DBMS_LOGMNR.START_LOGMNR(startScn=>?, endScn=>?, "
                + "options=>DBMS_LOGMNR.COMMITTED_DATA_ONLY+DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT)";
        CallableStatement cs = this.connection.prepareCall(sql);
        cs.setObject(1, startScn);
        cs.setObject(2, endScn);
        cs.execute();
        cs.close();
    }

    public void startLogMining(Long startScn) throws SQLException {
        startLogMining(startScn, null);
    }

    public void startLogMining() throws SQLException {
        startLogMining(null, null);
    }

    public void endLogMining() throws SQLException {
        CallableStatement cs = this.connection.prepareCall("call DBMS_LOGMNR.END_LOGMNR()");
        cs.execute();
        cs.close();
    }


    public List<LogEntry> fetchLogEntries(int size, long beginScn, FetchFilter[] fetchFilters) throws SQLException {
        LogEntry preLogEntry = null;
        List<LogEntry> logEntries = new ArrayList<>();
        String filterSql = "";
        String sql = "select scn, ssn, csf, operation, operation_code, RAWTOHEX(sql_redo) as sql_redo, table_name, seg_owner, start_scn,commit_scn " +
                "from v$logmnr_contents where operation_code in (1,2,3)";
        if (fetchFilters != null && fetchFilters.length > 0) {
            for (int i = 0; i < fetchFilters.length; i++) {
                if (i > 0) {
                    filterSql += " or ";

                }
                filterSql += "(seg_owner = '" + fetchFilters[i].getSchema() + "' and table_name = '" + fetchFilters[i].getTableName() + "')";
            }
            filterSql = " and (" + filterSql + ")";
        }
        sql += filterSql + " and scn > ? order by scn, ssn";

        PreparedStatement ps = this.connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(size);
        ps.setLong(1, beginScn);
        ResultSet rs = ps.executeQuery();
        try {
            while (rs.next()) {
                LogEntry logEntry = LogEntry.builder()
                        .scn(rs.getLong("scn"))
                        .ssn(rs.getInt("ssn"))
                        .csf(rs.getInt("csf"))
                        .operation(rs.getString("operation"))
                        .operationCode(rs.getInt("operation_code"))
                        .sqlRedo(rs.getString("sql_redo"))
                        .tableName(rs.getString("table_name"))
                        .segOwner(rs.getString("seg_owner"))
                        .startScn(rs.getLong("start_scn"))
                        .endScn(rs.getLong("commit_scn"))
                        .build();
                if (preLogEntry != null && preLogEntry.getScn() == logEntry.getScn()) {
                    logEntry.setSqlRedo(preLogEntry.getSqlRedo() + logEntry.getSqlRedo());
                }
                if (logEntry.getCsf() == 0) {
                    logEntry.setSqlRedo(StringUtils.hex2String(logEntry.getSqlRedo()));
                    logEntries.add(logEntry);
                }
                preLogEntry = logEntry;
                if (logEntries.size() == size) {
                    break;
                }
            }
        } finally {
            rs.close();
            ps.close();
        }
        return logEntries;
    }

    @Data
    @Builder
    public static class FetchFilter {
        private String schema;
        private String tableName;
    }
}