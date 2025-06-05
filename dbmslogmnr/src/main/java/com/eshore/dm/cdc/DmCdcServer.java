package com.eshore.dm.cdc;

import com.alibaba.fastjson2.JSONObject;
import com.eshore.dm.cdc.bean.ArchiveFile;
import com.eshore.dm.cdc.bean.ArchiveMetadata;
import com.eshore.dm.cdc.bean.DmlEntry;
import com.eshore.dm.cdc.bean.LogEntry;

import com.eshore.dm.cdc.util.DmlEntryConnector;
import com.eshore.dm.cdc.util.LogEntryProcessor;
import com.eshore.dm.cdc.util.LogMnr;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/28 11:26
 * @Description: TODO
 */
@Slf4j
public class DmCdcServer {

    public final static String ROOT = "./";
    public final static int NEW_START_SCN = -1;

    private LogEntryProcessor logEntryProcessor;
    private DmlEntryConnector dmlEntryConnector;
    private LogMnr logMnr;
    private DmDatasource srcDmDatasource;
    private DmDatasource destDmDatasource;

    public DmCdcServer() {
        this.logEntryProcessor = new LogEntryProcessor();
        this.srcDmDatasource = new DmDatasource("jdbc:dm://192.168.199.201:5236/userUnicode=true&characterEncoding=utf8", "SYSDBA", "SYSDBA");
        this.destDmDatasource = new DmDatasource("jdbc:dm://192.168.137.1:5236/userUnicode=true&characterEncoding=utf8", "SYSDBA", "SYSDBA");
    }

    public void saveArchiveFileMetadata(ArchiveMetadata archiveMetadata) {
        JSONObject jsonObject = JSONObject.from(archiveMetadata);
        try {
//            File file = new File(ROOT + "archiveFile.meta");
//            if (!file.exists()) {
//                file.createNewFile();
//            }
//            file.delete();
//            Files.write(Paths.get(ROOT + "archiveFile.meta"), jsonObject.getBytes("utf8"));
            IOUtils.write(jsonObject.toString(), new FileOutputStream(ROOT + "archiveFile.meta"), "utf8");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArchiveMetadata loadArchiveFileMetadata() {
        ArchiveMetadata archiveMetadata = null;
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(ROOT + "archiveFile.meta"));
            archiveMetadata = JSONObject.parseObject(new String(bytes, "utf8"), ArchiveMetadata.class);
        } catch (IOException e) {
            if (e instanceof NoSuchFileException) {
                log.info("no history archive file metadata");
            } else {
                log.error(e.getMessage(), e);
            }
        }
        return archiveMetadata;
    }

    public void run() {
        long currentScn;
        DmlEntry dmLEntry;
        List<DmlEntry> dmlEntries = new ArrayList<>();
        List<LogEntry> logEntries;
        ArchiveFile archiveFile;

        ArchiveMetadata archiveMetadata = loadArchiveFileMetadata();
        boolean isArchiveFileEnd = false;
        LogMnr.FetchFilter[] fetchFilters = new LogMnr.FetchFilter[]{
                LogMnr.FetchFilter.builder().schema("hz_dw").tableName("t_dwd_wo_form_all").build()
        };

        try {
            this.dmlEntryConnector = new DmlEntryConnector(this.destDmDatasource.getConnection());
            this.logMnr = new LogMnr(this.srcDmDatasource.getConnection());
            while (true) {
                if (archiveMetadata == null) {
                    archiveMetadata = ArchiveMetadata.builder().build();
                    archiveFile = this.logMnr.getActiveArchiveFile();
                    currentScn = NEW_START_SCN;
                } else {
                    if (isArchiveFileEnd) {
                        archiveFile = this.logMnr.getNextArchiveFile(archiveMetadata.getArchLSN());
                        currentScn = NEW_START_SCN;
                        log.info("change archive file, new file {}", archiveFile.getPath());
                        isArchiveFileEnd = false;
                    } else {
                        archiveFile = ArchiveFile.builder()
                                .archLSN(archiveMetadata.getArchLSN())
                                .path(archiveMetadata.getArchiveFilePath())
                                .status(this.logMnr.getArchiveFileStatus(archiveMetadata.getArchLSN()))
                                .build();
                        currentScn = archiveMetadata.getScn();
                    }
                }
                if (archiveFile == null) {
                    continue;
                }
                this.logMnr.addArchiveLogFile(archiveFile.getPath());
                if (currentScn == NEW_START_SCN) {
                    this.logMnr.startLogMining();
                } else {
                    this.logMnr.startLogMining(currentScn);
                }
                do {
                    dmlEntries.clear();
                    logEntries = this.logMnr.fetchLogEntries(500, currentScn, fetchFilters);
                    for (LogEntry logEntry : logEntries) {
                        currentScn = logEntry.getEndScn(); //commit的位置
                        dmLEntry = this.logEntryProcessor.process(logEntry);
                        dmlEntries.add(dmLEntry);
                    }
                    this.dmlEntryConnector.saveDmlEntries(dmlEntries);

                    archiveMetadata.setArchiveFilePath(archiveFile.getPath());
                    archiveMetadata.setArchLSN(archiveFile.getArchLSN());
                    archiveMetadata.setStatus(archiveFile.getStatus());
                    archiveMetadata.setScn(currentScn);
                    saveArchiveFileMetadata(archiveMetadata);
                } while (!logEntries.isEmpty());
                this.logMnr.endLogMining();
                if (logEntries.isEmpty() && archiveFile.getStatus().equalsIgnoreCase("inactive")) {
                    isArchiveFileEnd = true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DmCdcServer dmCdcServer = new DmCdcServer();
        log.info("开始运行");
        dmCdcServer.run();
    }
}
