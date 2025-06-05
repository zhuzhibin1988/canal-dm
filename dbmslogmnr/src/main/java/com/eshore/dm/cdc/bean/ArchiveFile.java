package com.eshore.dm.cdc.bean;

import lombok.Builder;
import lombok.Data;

/**
 * 保存归档文件元数据
 *
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/30 14:24
 * @Description: TODO
 */
@Data
@Builder
public class ArchiveFile {
    private long archLSN;
    private String path;
    private String status;

    public boolean equals(ArchiveFile archiveFile) {
        if (archiveFile == null) {
            return false;
        } else {
            return this.path.equals(archiveFile.getPath());
        }
    }
}
