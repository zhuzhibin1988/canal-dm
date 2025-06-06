package com.eshore.dm.cdc.bean;

import lombok.Builder;
import lombok.Data;

import java.sql.Types;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/6/6 15:43
 * @Description: TODO
 */
@Data
@Builder
public class ColumnValue {
    private String columnName;
    private Object columnValue;
    private Integer type;
}
