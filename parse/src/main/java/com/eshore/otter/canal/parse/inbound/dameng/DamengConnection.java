package com.eshore.otter.canal.parse.inbound.dameng;

import com.eshore.otter.canal.parse.driver.dameng.DamengRConnector;
import com.eshore.otter.canal.parse.driver.dameng.DamengRedoLogExecutor;
import com.eshore.otter.canal.parse.inbound.dameng.dbsync.DirectLogFetcher;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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


    public void connect() throws IOException {
        this.connector.connect();
    }

    public void reconnect() throws IOException {
        this.connector.reconnect();
    }

    public void disconnect() throws IOException {
        this.connector.disconnect();
    }

    public boolean isConnected() {
        return connector.isConnected();
    }

    public ResultSetPacket query(String cmd) throws IOException {
        DamengRedoLogExecutor exector = new DamengRedoLogExecutor(connector);
        return exector.query(cmd);
    }

    public List<ResultSetPacket> queryMulti(String cmd) throws IOException {
        MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
        return exector.queryMulti(cmd);
    }

    public void update(String cmd) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    /**
     * 加速主备切换时的查找速度，做一些特殊优化，比如只解析事务头或者尾
     */
    public void seek(String redoFilename, Long redPosition, SinkFunction func) throws IOException {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        // 若entry position存在gtid，则使用传入的gtid作为gtidSet
        // 拼接的标准,否则同时开启gtid和tsdb时，会导致丢失gtid
        // 而当源端数据库gtid 有purged时会有如下类似报错
        // 'errno = 1236, sqlstate = HY000 errmsg = The slave is connecting
        // using CHANGE MASTER TO MASTER_AUTO_POSITION = 1 ...
        if (StringUtils.isNotEmpty(gtid)) {
            GTIDSet gtidSet = parseGtidSet(gtid, isMariaDB());
            if (isMariaDB()) {
                decoder.handle(LogEvent.GTID_EVENT);
                decoder.handle(LogEvent.GTID_LIST_EVENT);
            } else {
                decoder.handle(LogEvent.GTID_LOG_EVENT);
            }
            context.setGtidSet(gtidSet);
        }
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
        while (fetcher.fetch()) {
            accumulateReceivedBytes(fetcher.limit());
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        updateSettings();
        loadBinlogChecksum();
        sendRegisterSlave();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
        while (fetcher.fetch()) {
            accumulateReceivedBytes(fetcher.limit());
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }

            if (event.getSemival() == 1) {
                sendSemiAck(context.getLogPosition().getFileName(), context.getLogPosition().getPosition());
            }
        }
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    public void dump(long timestamp, SinkFunction func) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(long timestamp, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    @Override
    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    private void sendRegisterSlave() throws IOException {
        RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
        SocketAddress socketAddress = connector.getChannel().getLocalSocketAddress();
        if (socketAddress == null || !(socketAddress instanceof InetSocketAddress)) {
            return;
        }

        InetSocketAddress address = (InetSocketAddress) socketAddress;
        String host = address.getHostString();
        int port = address.getPort();
        cmd.reportHost = host;
        cmd.reportPort = port;
        cmd.reportPasswd = authInfo.getPassword();
        cmd.reportUser = authInfo.getUsername();
        cmd.serverId = this.slaveId;
        byte[] cmdBody = cmd.toBytes();

        logger.info("Register slave {}", cmd);

        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(cmdBody.length);
        header.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), header.toBytes(), cmdBody);

        header = PacketManager.readHeader(connector.getChannel(), 4);
        byte[] body = PacketManager.readBytes(connector.getChannel(), header.getPacketBodyLength());
        assert body != null;
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Register slave:" + err.toString());
            } else {
                throw new IOException("Unexpected packet with field_count=" + body[0]);
            }
        }
    }

    private void sendBinlogDump(String binlogfilename, Long binlogPosition) throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = this.slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
        connector.setDumping(true);
    }

    public void sendSemiAck(String binlogfilename, Long binlogPosition) throws IOException {
        SemiAckCommandPacket semiAckCmd = new SemiAckCommandPacket();
        semiAckCmd.binlogFileName = binlogfilename;
        semiAckCmd.binlogPosition = binlogPosition;

        byte[] cmdBody = semiAckCmd.toBytes();

        logger.info("SEMI ACK with position:{}", semiAckCmd);
        HeaderPacket semiAckHeader = new HeaderPacket();
        semiAckHeader.setPacketBodyLength(cmdBody.length);
        semiAckHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), semiAckHeader.toBytes(), cmdBody);
    }

    private void sendBinlogDumpGTID(GTIDSet gtidSet) throws IOException {
        if (isMariaDB()) {
            sendMariaBinlogDumpGTID(gtidSet);
        } else {
            sendMySQLBinlogDumpGTID(gtidSet);
        }
    }

    private void sendMySQLBinlogDumpGTID(GTIDSet gtidSet) throws IOException {
        BinlogDumpGTIDCommandPacket binlogDumpCmd = new BinlogDumpGTIDCommandPacket();
        binlogDumpCmd.slaveServerId = this.slaveId;
        binlogDumpCmd.gtidSet = gtidSet;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP_GTID:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
        connector.setDumping(true);
    }

    private void sendMariaBinlogDumpGTID(GTIDSet gtidSet) throws IOException {
        update("SET @slave_connect_state = '" + new String(gtidSet.encode()) + "'");
        update("SET @slave_gtid_strict_mode = 0");
        update("SET @slave_gtid_ignore_duplicates = 0");
        sendRegisterSlave();
        sendBinlogDump("", 0L);
        connector.setDumping(true);
    }

    public DamengConnection fork() {
        DamengConnection connection = new DamengConnection();
        connection.setCharset(getCharset());
        connection.setSlaveId(getSlaveId());
        connection.setConnector(connector.fork());
        // set authInfo
        connection.setAuthInfo(authInfo);
        return connection;
    }

    @Override
    public long queryServerId() throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    // ====================== help method ====================






    private void accumulateReceivedBytes(long x) {
        if (receivedBinlogBytes != null) {
            receivedBinlogBytes.addAndGet(x);
        }
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

    public BinlogFormat getBinlogFormat() {
        if (binlogFormat == null) {
            synchronized (this) {
                loadBinlogFormat();
            }
        }

        return binlogFormat;
    }

    public BinlogImage getBinlogImage() {
        if (binlogImage == null) {
            synchronized (this) {
                loadBinlogImage();
            }
        }

        return binlogImage;
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

    public void setReceivedBinlogBytes(AtomicLong receivedBinlogBytes) {
        this.receivedBinlogBytes = receivedBinlogBytes;
    }

    public boolean isMariaDB() {
        return connector.getServerVersion() != null && connector.getServerVersion().toLowerCase().contains("mariadb");
    }

}
