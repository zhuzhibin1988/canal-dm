package com.eshore.dm.cdc.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/30 14:24
 * @Description: TODO
 */
@Data
@Builder
public class LogEntry {
    private long scn;
    private int ssn;
    private int csf;
    private long startScn;
    private long endScn;
    private String operation;
    private int operationCode;
    private String segOwner;
    private String tableName;
    private String sqlRedo;
}
