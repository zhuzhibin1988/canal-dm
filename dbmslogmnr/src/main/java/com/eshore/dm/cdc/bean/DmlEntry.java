package com.eshore.dm.cdc.bean;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/6/4 15:00
 * @Description: TODO
 */
@Data
@Builder
public class DmlEntry {
    private String schemaName;
    private String tableName;
    private String dmlType;
    private List<String> primaryKeys;
    private List<ColumnValue> primaryKeyValues;
    private List<ColumnValue> columnValues;
    private String dml;
}
