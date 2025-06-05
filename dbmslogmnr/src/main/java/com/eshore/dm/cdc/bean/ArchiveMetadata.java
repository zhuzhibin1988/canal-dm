package com.eshore.dm.cdc.bean;

import lombok.Builder;
import lombok.Data;

/**
 * 保存最新同步信息元数据
 *
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/30 14:23
 * @Description: TODO
 */
@Data
@Builder
public class ArchiveMetadata {
    private long scn;
    private long archLSN;
    private String archiveFilePath;
    private String status;
}
