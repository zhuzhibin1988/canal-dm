package com.eshore.dm.cdc.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.eshore.dm.cdc.bean.ColumnValue;
import com.eshore.dm.cdc.bean.DmlEntry;
import com.eshore.dm.cdc.bean.LogEntry;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public DmlEntry process(LogEntry logEntry, Map<String, Integer> typesMap) {
        DmlEntry dmlEntry = null;
        int operationCode = logEntry.getOperationCode();
        if (operationCode == 1) {
            dmlEntry = processInsert(logEntry.getSqlRedo(), typesMap);
        } else if (operationCode == 2) {
            dmlEntry = processDelete(logEntry.getSqlRedo(), typesMap);
        } else if (operationCode == 3) {
            dmlEntry = processUpdate(logEntry.getSqlRedo(), typesMap);
        }
        return dmlEntry;
    }

    private DmlEntry processInsert(String insertSql, Map<String, Integer> typesMap) {
        SQLStatementParser sqlStatementParser = new SQLStatementParser(insertSql);
        SQLInsertStatement statement = (SQLInsertStatement) sqlStatementParser.parseInsert();
        List<SQLExpr> columns = statement.getColumns();
        List<SQLExpr> values = statement.getValues().getValues();
        List<ColumnValue> columnValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String name = StringUtils.cleanColumn(columns.get(i).toString());
            Integer type = typesMap.get(name);
            Object value = cleanTimeLiteral(values.get(i), type);
            columnValues.add(ColumnValue.builder().columnName(name).columnValue(value).type(type).build());
        }

        DmlEntry dmlEntry = DmlEntry.builder()
                .schemaName("\"hz2_dw\"")
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("INSERT")
                .primaryKeys(Collections.emptyList())
                .columnValues(columnValues)
                .dml(insertSql)
                .build();
        return dmlEntry;
    }

    private DmlEntry processDelete(String deleteSql, Map<String, Integer> typesMap) {
        SQLStatementParser sqlStatementParser = new SQLStatementParser(deleteSql);
        SQLDeleteStatement statement = sqlStatementParser.parseDeleteStatement();

        SQLExpr where = statement.getWhere();
        List<ColumnValue> primaryKeyValues = new ArrayList<>();
        this.pkAst2List(where, primaryKeyValues, typesMap);

        DmlEntry dmlEntry = DmlEntry.builder()
                .schemaName("\"hz2_dw\"")
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("DELETE")
                .primaryKeyValues(primaryKeyValues)
                .dml(deleteSql)
                .build();
        return dmlEntry;
    }

    private DmlEntry processUpdate(String updateSql, Map<String, Integer> typesMap) {
        SQLStatementParser sqlStatementParser = new SQLStatementParser(updateSql);
        SQLUpdateStatement statement = sqlStatementParser.parseUpdateStatement();
        List<SQLUpdateSetItem> setItems = statement.getItems();
        List<ColumnValue> columnValues = new ArrayList<>();
        for (int i = 0; i < setItems.size(); i++) {
            String name = StringUtils.cleanColumn(setItems.get(i).getColumn().toString());
            Integer type = typesMap.get(name);
            Object value = cleanTimeLiteral(setItems.get(i).getValue(), type);
            columnValues.add(ColumnValue.builder().columnName(name).columnValue(value).type(type).build());
        }
        SQLExpr where = statement.getWhere();
        List<ColumnValue> primaryKeyValues = new ArrayList<>();
        this.pkAst2List(where, primaryKeyValues, typesMap);

        DmlEntry dmlEntry = DmlEntry.builder()
                .schemaName("\"hz2_dw\"")
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("UPDATE")
                .columnValues(columnValues)
                .primaryKeyValues(primaryKeyValues)
                .dml(updateSql)
                .build();
        return dmlEntry;
    }

    String cleanTimeLiteral(Object columnValue, Integer type) {
        StringBuffer result = new StringBuffer();
        if (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
            Matcher matcher = DATE_LITERAL_PATTER.matcher(columnValue.toString());
            // 替换匹配部分为只保留单引号和时间内容
            while (matcher.find()) {
                String timeValue = matcher.group(1); // 捕获时间字符串部分
                matcher.appendReplacement(result, timeValue); // 只保留时间值（包含单引号）
            }
            matcher.appendTail(result);
            return result.toString();
        } else {
            return columnValue.toString();
        }
    }

    private void pkAst2List(SQLExpr sqlExpr, List<ColumnValue> pkList, Map<String, Integer> typesMap) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            if (sqlBinaryOpExpr.getLeft() instanceof SQLIdentifierExpr) {
                String name = StringUtils.cleanColumn(sqlBinaryOpExpr.getLeft().toString());
                Integer type = typesMap.get(name);
                Object value = cleanTimeLiteral(sqlBinaryOpExpr.getRight(), type);
                pkList.add(ColumnValue.builder().columnName(name).columnValue(value).type(type).build());
            } else {
                pkAst2List(sqlBinaryOpExpr.getLeft(), pkList, typesMap);
                pkAst2List(sqlBinaryOpExpr.getRight(), pkList, typesMap);
            }
        }
    }
}
