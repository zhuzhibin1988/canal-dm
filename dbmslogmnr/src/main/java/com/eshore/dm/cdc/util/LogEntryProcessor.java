package com.eshore.dm.cdc.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.eshore.dm.cdc.bean.DmlEntry;
import com.eshore.dm.cdc.bean.LogEntry;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/30 14:30
 * @Description: TODO
 */
public final class LogEntryProcessor {

    // 正则表达式：匹配 TIMESTAMP 或 DATE 后跟日期/时间字符串
    public static final Pattern DATE_LITERAL_PATTER = Pattern.compile("(?:TIMESTAMP|DATE)\\s?('(?:\\d{4}-\\d{2}-\\d{2}(?:\\s+\\d{2}:\\d{2}:\\d{2})?)')");

    private Object setClobValue(Object object) {
        if (object.toString().equalsIgnoreCase("empty_clob")) {
            return "";
        } else if (object.toString().equalsIgnoreCase("out_clob")) {
            return "'out_clob'";
        }
        return object;
    }

    public DmlEntry process(LogEntry logEntry) {
        DmlEntry dmlEntry = null;
        int operationCode = logEntry.getOperationCode();
        if (operationCode == 1) {
            dmlEntry = processInsert(logEntry.getSqlRedo());
        } else if (operationCode == 2) {
            dmlEntry = processDelete(logEntry.getSqlRedo());
        } else if (operationCode == 3) {
            dmlEntry = processUpdate(logEntry.getSqlRedo());
        }
        return dmlEntry;
    }

    private DmlEntry processInsert(String insertSql) {
        insertSql = formatSql(insertSql);
        SQLStatementParser sqlStatementParser = new SQLStatementParser(insertSql);
        SQLInsertStatement statement = (SQLInsertStatement) sqlStatementParser.parseInsert();
        List<SQLExpr> columns = statement.getColumns();
        List<SQLExpr> values = statement.getValues().getValues();
        List<Pair<String, Object>> columnValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnValues.add(Pair.of(columns.get(i).toString(), this.setClobValue(values.get(i))));
        }

        DmlEntry dmlEntry = DmlEntry.builder()
                .schemaName("\"hz_dw\"")
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("INSERT")
                .primaryKeys(Collections.emptyList())
                .columnValues(columnValues)
                .dml(insertSql)
                .build();
        return dmlEntry;
    }

    private DmlEntry processDelete(String deleteSql) {
        deleteSql = formatSql(deleteSql);
        SQLStatementParser sqlStatementParser = new SQLStatementParser(deleteSql);
        SQLDeleteStatement statement = sqlStatementParser.parseDeleteStatement();

        SQLExpr where = statement.getWhere();
        List<Pair<String, Object>> primaryKeyValues = new ArrayList<>();
        this.pkAst2List(where, primaryKeyValues);

        DmlEntry dmlEntry = DmlEntry.builder()
                .schemaName("\"hz_dw\"")
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("DELETE")
                .primaryKeyValues(primaryKeyValues)
                .dml(deleteSql)
                .build();
        return dmlEntry;
    }

    private DmlEntry processUpdate(String updateSql) {
        updateSql = formatSql(updateSql);
        SQLStatementParser sqlStatementParser = new SQLStatementParser(updateSql);
        SQLUpdateStatement statement = sqlStatementParser.parseUpdateStatement();
        List<SQLUpdateSetItem> setItems = statement.getItems();
        List<Pair<String, Object>> columnValues = new ArrayList<>();
        for (int i = 0; i < setItems.size(); i++) {
            columnValues.add(Pair.of(setItems.get(i).getColumn().toString(), setItems.get(i).getValue()));
        }
        SQLExpr where = statement.getWhere();
        List<Pair<String, Object>> primaryKeyValues = new ArrayList<>();
        this.pkAst2List(where, primaryKeyValues);

        DmlEntry dmlEntry = DmlEntry.builder()
                .schemaName("\"hz_dw\"")
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("UPDATE")
                .columnValues(columnValues)
                .primaryKeyValues(primaryKeyValues)
                .dml(updateSql)
                .build();
        return dmlEntry;
    }

    String formatSql(String sql) {
        Matcher matcher = DATE_LITERAL_PATTER.matcher(sql);
        // 替换匹配部分为只保留单引号和时间内容
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String timeValue = matcher.group(1); // 捕获时间字符串部分
            matcher.appendReplacement(result, timeValue); // 只保留时间值（包含单引号）
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private void pkAst2List(SQLExpr sqlExpr, List<Pair<String, Object>> pkList) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            if (sqlBinaryOpExpr.getLeft() instanceof SQLIdentifierExpr) {
                pkList.add(Pair.of(sqlBinaryOpExpr.getLeft().toString(), sqlBinaryOpExpr.getRight()));
            } else {
                pkAst2List(sqlBinaryOpExpr.getLeft(), pkList);
                pkAst2List(sqlBinaryOpExpr.getRight(), pkList);
            }
        }
    }
}
