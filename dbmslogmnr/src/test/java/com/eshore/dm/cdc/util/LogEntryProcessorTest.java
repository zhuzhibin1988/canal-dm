package com.eshore.dm.cdc.util;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.eshore.dm.cdc.bean.DmlEntry;
import com.eshore.dm.cdc.bean.LogEntry;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/6/3 17:51
 * @Description: TODO
 */

public class LogEntryProcessorTest {
    private LogEntryProcessor logEntryProcessor;

    @Before
    public void setup() {
        this.logEntryProcessor = new LogEntryProcessor();
    }

    @Test
    public void testProcessLogEntry() {
        LogEntryProcessor logEntryProcessor = new LogEntryProcessor();
        String sql = "INSERT INTO \"XC_ZXSB\".\"interface_log\"(\"LOG_ID\", \"OBJ_ID\", \"OBJ_TYPE\", \"CLASS_NAME\", \"METHOD_NAME\", \"CREATE_DATE\", \"OPERATOR\", " +
                "\"REQUEST_DATA\", \"RESPONSE_DATA\") VALUES (1089790124317466624, 1881975454032539651, 'wb_upload_file', 'com.eshore.web.controller.UploadDocWebController', " +
                "'uploadMe', DATE'2025-06-03', '440784199504220612', '_24332000000446194601 (1).pdf', '{\"code\":\"0000\",\"docid\":\"445096a9-4051-11f0-826a-60da833fd9c3\"," +
                "\"downloadUrl\":\"https://ap.gzonline.gov.cn/WebDiskServerDemo/doc?doc_id=445096a9-4051-11f0-826a-60da833fd9c3\",\"msg\":\"OK\",\"uuid\":\"445096a9-4051-11f0-826a-60da833fd9c3\"}')";
        sql = logEntryProcessor.formatSql(sql);
        SQLStatementParser sqlStatementParser = new SQLStatementParser(sql, DbType.dm);
        SQLInsertStatement statement = (SQLInsertStatement) sqlStatementParser.parseInsert();
        List<SQLExpr> columns = statement.getColumns();
        List<SQLExpr> values = statement.getValues().getValues();
        List<Pair<String, Object>> columnValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnValues.add(Pair.of(columns.get(i).toString(), values.get(i)));
        }
        DmlEntry dmlEntry = DmlEntry.builder()
                .tableName(statement.getTableName().getSimpleName())
                .dmlType("INSERT")
                .primaryKeys(Collections.EMPTY_LIST)
                .columnValues(columnValues)
                .dml(sql)
                .build();
        System.out.println(dmlEntry);
    }

    @Test
    public void testProcess() {
        LogEntry logEntry = LogEntry.builder()
                .sqlRedo("DELETE FROM \"hz_dw\".\"t_dwd_wo_form_all\" WHERE \"id\" = 'ff4d4e340ae1a373144a1c7343fbb175' AND \"is_re_handle_branch\" = 0 AND \"create_time\" = " +
                        "'2024-01-26 09:07:45'")
                .operationCode(2)
                .build();
        DmlEntry dmlEntry = logEntryProcessor.process(logEntry);
        System.out.println(dmlEntry);
    }
}
