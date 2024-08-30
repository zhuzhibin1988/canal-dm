package com.eshore.otter.canal.parse.driver.dameng;

import com.eshore.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.eshore.otter.canal.parse.driver.mysql.packets.client.QueryCommandPacket;
import com.eshore.otter.canal.parse.driver.mysql.packets.server.*;
import com.eshore.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.eshore.otter.canal.parse.driver.mysql.utils.PacketManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 执行sql（包括start_logmnr,add_logfile,end_logmnr,查询归档日志）
 *
 * @author zhuzhibin
 * @since 1.0.0
 */
public class DamengRedoLogExecutor {

    private DamengConnector connector;

    public DamengRedoLogExecutor(DamengConnector connector) throws SQLException {
        if (!connector.isConnected()) {
            throw new SQLException("should execute connector.connect() first");
        }
        this.connector = connector;
    }


    public void initArchiveFileForMining() {

    }

    public LogFile queryRedoLogFile() throws SQLException {
        String sql = SqlUtils.FILES_FOR_MINING;
        PreparedStatement ps = this.connector.connection().prepareStatement(sql);
        return ps.executeQuery();
    }

    public void addLogFile(LogFile logFile) throws SQLException {
        String sql = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", logFile.getFileName());
        connector.connection().createStatement().execute(sql);
    }

    public void startLogmnr(Scn startScn, Scn endScn) throws SQLException {
        String sql = SqlUtils.startLogMinerStatement(startScn, endScn);
        connector.connection().createStatement().execute(sql);
    }

    public void endLogmnr() throws SQLException {
        this.connector.connection().createStatement().execute(SqlUtils.END_LOGMNR);
    }

    public void removeLogFile(LogFile logFile) throws SQLException {
        String sql = SqlUtils.deleteLogFileStatement(logFile.getFileName());
        this.connector.connection().createStatement().execute(sql);
    }
}
