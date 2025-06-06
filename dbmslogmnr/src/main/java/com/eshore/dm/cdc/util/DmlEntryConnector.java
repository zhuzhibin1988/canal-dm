package com.eshore.dm.cdc.util;

import com.eshore.dm.cdc.bean.ColumnValue;
import com.eshore.dm.cdc.bean.DmlEntry;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/6/3 18:03
 * @Description: TODO
 */

@Slf4j
public class DmlEntryConnector {
    private final Connection connection;

    public DmlEntryConnector(Connection connection) {
        this.connection = connection;
        try {
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveDmlEntries(List<DmlEntry> dmlEntries) throws SQLException {
        String sql = null;
        for (DmlEntry dmlEntry : dmlEntries) {
            if (dmlEntry.getDmlType().equalsIgnoreCase("insert")) {
                sql = this.getInsertSql(dmlEntry);
            } else if (dmlEntry.getDmlType().equalsIgnoreCase("delete")) {
                sql = this.getDeleteSql(dmlEntry);
            } else if (dmlEntry.getDmlType().equalsIgnoreCase("update")) {
                sql = this.getUpdateSql(dmlEntry);
            }
//            log.info(sql);
            PreparedStatement pstmt = this.connection.prepareStatement(sql);
            pstmt.execute(sql);
            pstmt.close();
        }
        this.connection.commit();
    }

    private String getInsertSql(DmlEntry dmlEntry) {
        StringBuilder insertBuilder = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (ColumnValue columnValue : dmlEntry.getColumnValues()) {
            columns.append(columnValue.getColumnName()).append(",");
            values.append(columnValue.getColumnValue()).append(",");
        }
        int len = columns.length();
        columns.delete(len - 1, len);
        len = values.length();
        values.delete(len - 1, len);

        insertBuilder.append("insert into ").append(dmlEntry.getSchemaName()).append(".").append(dmlEntry.getTableName())
                .append(" (").append(columns).append(") values (").append(values).append(")");
        return insertBuilder.toString();
    }

    private String getDeleteSql(DmlEntry dmlEntry) {
        StringBuilder deleteBuilder = new StringBuilder();
        StringBuilder whereBuilder = new StringBuilder();

        for (ColumnValue primaryKeyValues : dmlEntry.getPrimaryKeyValues()) {
            whereBuilder.append(primaryKeyValues.getColumnName()).append(" = ").append(primaryKeyValues.getColumnName()).append(" and ");
        }
        int len = whereBuilder.length();
        whereBuilder.delete(len - 4, len);

        deleteBuilder.append("delete from ").append(dmlEntry.getSchemaName()).append(".").append(dmlEntry.getTableName())
                .append(" where ").append(whereBuilder);
        return deleteBuilder.toString();
    }

    private String getUpdateSql(DmlEntry dmlEntry) {
        StringBuilder updateBuilder = new StringBuilder();
        StringBuilder whereBuilder = new StringBuilder();
        StringBuilder setBuilder = new StringBuilder();

        for (ColumnValue columnValue : dmlEntry.getColumnValues()) {
            setBuilder.append(columnValue.getColumnName()).append(" = ").append(columnValue.getColumnValue()).append(",");
        }
        int len = setBuilder.length();
        setBuilder.delete(len - 1, len);

        for (ColumnValue primaryKeyValues : dmlEntry.getPrimaryKeyValues()) {
            whereBuilder.append(primaryKeyValues.getColumnName()).append(" = ").append(primaryKeyValues.getColumnValue()).append(" and ");
        }
        len = whereBuilder.length();
        whereBuilder.delete(len - 4, len);

        updateBuilder.append("update ").append(dmlEntry.getSchemaName()).append(".").append(dmlEntry.getTableName())
                .append(" set ").append(setBuilder)
                .append(" where ").append(whereBuilder);
        return updateBuilder.toString();
    }
}