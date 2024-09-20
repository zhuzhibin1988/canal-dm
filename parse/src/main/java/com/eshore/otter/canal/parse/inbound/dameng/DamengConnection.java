package com.eshore.otter.canal.parse.inbound.dameng;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.eshore.otter.canal.parse.driver.dameng.DamengConnector;
import com.eshore.otter.canal.parse.driver.dameng.DamengQueryExecutor;
import com.eshore.otter.canal.parse.driver.dameng.DamengUpdateExecutor;
import com.eshore.otter.canal.parse.inbound.dameng.dbsync.DirectLogFetcher;
import com.eshore.dbsync.logminer.LogDecoder;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;

import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DamengConnection implements ErosaConnection {

    private static final Logger logger = LoggerFactory.getLogger(DamengConnection.class);

    private DamengConnector connector;
    private Charset charset = Charset.forName("UTF-8");

    // tsdb releated
    private AuthenticationInfo authInfo;
    protected int connTimeout = 5 * 1000;                                      // 5秒
    protected int soTimeout = 60 * 60 * 1000;                                // 1小时

    public DamengConnection() {
    }

    public DamengConnection(InetSocketAddress address, String username, String password) {
        this.authInfo = new AuthenticationInfo();
        this.authInfo.setAddress(address);
        this.authInfo.setUsername(username);
        this.authInfo.setPassword(password);
        this.connector = new DamengConnector(address, username, password);
    }

    @Override
    public void connect() throws IOException {
        try {
            this.connector.connect();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void reconnect() throws IOException {
        try {
            this.connector.reconnect();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void disconnect() throws IOException {
        try {
            this.connector.disconnect();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }


    public boolean isConnected() {
        try {
            return this.connector.isConnected();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public ResultSet query(String sql) throws SQLException {
        DamengQueryExecutor exector = new DamengQueryExecutor(this.connector);
        return exector.query(sql);
    }

    public void update(String sql) throws SQLException {
        DamengUpdateExecutor exector = new DamengUpdateExecutor(this.connector);
        exector.update(sql);
    }

    @Override
    public void seek(String s, Long aLong, String s1, SinkFunction sinkFunction) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(String archiveFilename,, Long scnPosition, SinkFunction func) throws IOException {
        try {
            doDump(archiveFilename, scnPosition, func);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void doDump(String redoLogFilename, Long scnPosition, SinkFunction func) throws Exception {
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(this.connector);
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        while (fetcher.fetch()) {
            LogEvent event = decoder.decode(fetcher, context);
            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(long timestamp, SinkFunction func) {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(long timestamp, MultiStageCoprocessor coprocessor) {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public ErosaConnection fork() {
        return null;
    }

    @Override
    public long queryServerId() {
        throw new NullPointerException("Not implement yet");
    }

    // ================== setter / getter ===================

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public DamengConnector getConnector() {
        return this.connector;
    }

    public void setConnector(DamengConnector connector) {
        this.connector = connector;
    }

    public InetSocketAddress getAddress() {
        return authInfo.getAddress();
    }

    public void setConnTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    public void setAuthInfo(AuthenticationInfo authInfo) {
        this.authInfo = authInfo;
    }
}
