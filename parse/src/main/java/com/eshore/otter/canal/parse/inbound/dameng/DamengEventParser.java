package com.eshore.otter.canal.parse.inbound.dameng;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.eshore.dbsync.logminer.event.dameng.RedoLog;
import com.eshore.dbsync.logminer.event.dameng.Scn;
import com.eshore.otter.canal.parse.inbound.dameng.dbsync.LogEventConvert;
import com.eshore.otter.canal.parse.inbound.dameng.dbsync.TableMetaCache;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于向mysql server复制binlog实现
 *
 * <pre>
 * 1. 自身不控制mysql主备切换，由ha机制来控制. 比如接入tddl/cobar/自身心跳包成功率
 * 2. 切换机制
 * </pre>
 *
 * @author jianghang 2012-6-21 下午04:06:32
 * @version 1.0.0
 */
public class DamengEventParser extends AbstractDamengEventParser implements CanalEventParser, CanalHASwitchable {

    private CanalHAController haController = null;

    private int defaultConnectionTimeoutInSeconds = 30;       // sotimeout
    private int receiveBufferSize = 64 * 1024;
    private int sendBufferSize = 64 * 1024;
    // 数据库信息
    protected AuthenticationInfo masterInfo;                                   // 主库
    protected AuthenticationInfo standbyInfo;                                  // 备库
    // binlog信息
    protected EntryPosition masterPosition;
    protected EntryPosition standbyPosition;
    // 心跳检查信息
    private String detectingSQL;                                 // 心跳sql
    private DamengConnection metaConnection;                               // 查询meta信息的链接
    private TableMetaCache tableMetaCache;                               // 对应meta
    private int fallbackIntervalInSeconds = 60;       // 切换回退时间

    // update by yishun.chen,特殊异常处理参数
    private boolean rdsOssMode = false;
    private boolean autoResetLatestPosMode = false;    // true:
    // binlog被删除之后，自动按最新的数据订阅

    private boolean multiStreamEnable;//support for polardbx binlog-x

    protected ErosaConnection buildErosaConnection() {
        return buildDamengConnection(this.runningInfo);
    }

    protected void preDump(ErosaConnection connection) {
        if (!(connection instanceof DamengConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (redoLogParser != null && redoLogParser instanceof LogEventConvert) {
            metaConnection = (DamengConnection) connection.fork();
            try {
                metaConnection.connect();
            } catch (IOException e) {
                throw new CanalParseException(e);
            }

            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setConnection(metaConnection);
                ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
                ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
                ((DatabaseTableMeta) tableMetaTSDB).setSnapshotInterval(tsdbSnapshotInterval);
                ((DatabaseTableMeta) tableMetaTSDB).setSnapshotExpire(tsdbSnapshotExpire);
                ((DatabaseTableMeta) tableMetaTSDB).init(destination);
            }

            tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
            ((LogEventConvert) redoLogParser).setTableMetaCache(tableMetaCache);
        }
    }

    protected void afterDump(ErosaConnection connection) {
        super.afterDump(connection);

        if (connection == null) {
            throw new CanalParseException("illegal connection is null");
        }

        if (!(connection instanceof DamengConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector()
                        .getAddress(), e);
            }
        }
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }

        super.start();
    }

    public void stop() throws CanalParseException {
        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector().getAddress(), e);
            }
        }

        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }

        super.stop();
    }

    protected TimerTask buildHeartBeatTimeTask(ErosaConnection connection) {
        if (!(connection instanceof DamengConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        // 开始mysql心跳sql
        if (detectingEnable && StringUtils.isNotBlank(detectingSQL)) {
            return new DamengDetectingTimeTask((DamengConnection) connection.fork());
        } else {
            return super.buildHeartBeatTimeTask(connection);
        }

    }

    protected void stopHeartBeat() {
        TimerTask heartBeatTimerTask = this.heartBeatTimerTask;
        super.stopHeartBeat();
        if (heartBeatTimerTask != null && heartBeatTimerTask instanceof DamengDetectingTimeTask) {
            DamengConnection damengConnection = ((DamengDetectingTimeTask) heartBeatTimerTask).getDamengConnection();
            try {
                damengConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect heartbeat connection for address:{}", damengConnection.getConnector()
                        .getAddress(), e);
            }
        }
    }

    /**
     * 心跳信息
     *
     * @author jianghang 2012-7-6 下午02:50:15
     * @version 1.0.0
     */
    class DamengDetectingTimeTask extends TimerTask {

        private boolean reconnect = false;
        private DamengConnection damengConnection;

        public DamengDetectingTimeTask(DamengConnection damengConnection) {
            this.damengConnection = damengConnection;
        }

        public void run() {
            try {
                if (reconnect) {
                    reconnect = false;
                    damengConnection.reconnect();
                } else if (!damengConnection.isConnected()) {
                    damengConnection.connect();
                }
                long startTime = System.currentTimeMillis();

                // 可能心跳sql为select 1
                if (StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "select")
                        || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "show")
                        || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "explain")
                        || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "desc")) {
                    damengConnection.query(detectingSQL);
                } else {
                    damengConnection.update(detectingSQL);
                }

                long costTime = System.currentTimeMillis() - startTime;
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onSuccess(costTime);
                }
            } catch (Throwable e) {
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onFailed(e);
                }
                reconnect = true;
                logger.warn("connect failed by ", e);
            }

        }

        public DamengConnection getDamengConnection() {
            return damengConnection;
        }
    }

    // 处理主备切换的逻辑
    public void doSwitch() {
        AuthenticationInfo newRunningInfo = (runningInfo.equals(masterInfo) ? standbyInfo : masterInfo);
        this.doSwitch(newRunningInfo);
    }

    public void doSwitch(AuthenticationInfo newRunningInfo) {
        // 1. 需要停止当前正在复制的过程
        // 2. 找到新的position点
        // 3. 重新建立链接，开始复制数据
        // 切换ip
        String alarmMessage = null;

        if (this.runningInfo.equals(newRunningInfo)) {
            alarmMessage = "same runingInfo switch again : " + runningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            return;
        }

        if (newRunningInfo == null) {
            alarmMessage = "no standby config, just do nothing, will continue try:"
                    + runningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            sendAlarm(destination, alarmMessage);
            return;
        } else {
            stop();
            alarmMessage = "try to ha switch, old:" + runningInfo.getAddress().toString() + ", new:"
                    + newRunningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            sendAlarm(destination, alarmMessage);
            runningInfo = newRunningInfo;
            start();
        }
    }

    // =================== helper method =================

    private DamengConnection buildDamengConnection(AuthenticationInfo runningInfo) {
        DamengConnection connection = new DamengConnection(runningInfo.getAddress(),
                runningInfo.getUsername(),
                runningInfo.getPassword());
        connection.setCharset(connectionCharset);
        return connection;
    }

    protected EntryPosition findStartPosition(ErosaConnection connection) throws IOException {
        EntryPosition startPosition = findStartPositionInternal(connection);
        if (needTransactionPosition.get()) {
            logger.warn("prepare to find last position : {}", startPosition.toString());
            Long preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition);
            if (!preTransactionStartPosition.equals(startPosition.getPosition())) {
                logger.warn("find new start Transaction Position , old : {} , new : {}",
                        startPosition.getPosition(),
                        preTransactionStartPosition);
                startPosition.setPosition(preTransactionStartPosition);
            }
            needTransactionPosition.compareAndSet(true, false);
        }
        return startPosition;
    }

    protected EntryPosition findEndPosition(ErosaConnection connection) throws IOException {
        DamengConnection damengConnection = (DamengConnection) connection;
        EntryPosition endPosition = findEndPosition(damengConnection);
        return endPosition;
    }

    protected EntryPosition findEndPositionWithMasterIdAndTimestamp(ErosaConnection connection) {
        DamengConnection damengConnection = (DamengConnection) connection;
        final EntryPosition endPosition = findEndPosition(damengConnection);
        if (tableMetaTSDB != null) {
            long startTimestamp = System.currentTimeMillis();
            return findAsPerTimestampInSpecificLogFile(damengConnection,
                    startTimestamp,
                    endPosition,
                    endPosition.getJournalName(),
                    true);
        } else {
            return endPosition;
        }
    }

    protected EntryPosition findPositionWithMasterIdAndTimestamp(ErosaConnection connection, EntryPosition fixedPosition) {
        DamengConnection damengConnection = (DamengConnection) connection;
        if (tableMetaTSDB != null && (fixedPosition.getTimestamp() == null || fixedPosition.getTimestamp() <= 0)) {
            // 使用一个未来极大的时间，基于位点进行定位
            long startTimestamp = System.currentTimeMillis() + 102L * 365 * 24 * 3600 * 1000; // 当前时间的未来102年
            EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(damengConnection,
                    startTimestamp,
                    fixedPosition,
                    fixedPosition.getJournalName(),
                    true);
            if (entryPosition == null) {
                throw new CanalParseException("[fixed timestamp] can't found begin/commit position before with fixed position "
                        + fixedPosition.getJournalName() + ":" + fixedPosition.getPosition());
            }
            return entryPosition;
        } else {
            return fixedPosition;
        }
    }

    protected EntryPosition findStartPositionInternal(ErosaConnection connection) {
        DamengConnection damengConnection = (DamengConnection) connection;
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录
            EntryPosition entryPosition = null;
            if (masterInfo != null && damengConnection.getConnector().getAddress().equals(masterInfo.getAddress())) {
                entryPosition = masterPosition;
            } else if (standbyInfo != null
                    && damengConnection.getConnector().getAddress().equals(standbyInfo.getAddress())) {
                entryPosition = standbyPosition;
            }

            if (entryPosition == null) {
                entryPosition =
                        findEndPositionWithMasterIdAndTimestamp(damengConnection); // 默认从当前最后一个位置进行消费
            }

            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                    logger.warn("prepare to find start position {}:{}:{}",
                            new Object[]{"", "", entryPosition.getTimestamp()});
                    return findByStartTimeStamp(damengConnection, entryPosition.getTimestamp());
                } else {
                    logger.warn("prepare to find start position just show master status");
                    return findEndPositionWithMasterIdAndTimestamp(damengConnection); // 默认从当前最后一个位置进行消费
                }
            } else {
                if (entryPosition.getPosition() != null && entryPosition.getPosition() > 0L) {
                    // 如果指定binlogName + offest，直接返回
                    entryPosition = findPositionWithMasterIdAndTimestamp(damengConnection, entryPosition);
                    logger.warn("prepare to find start position {}:{}:{}",
                            new Object[]{entryPosition.getJournalName(), entryPosition.getPosition(),
                                    entryPosition.getTimestamp()});
                    return entryPosition;
                } else {
                    EntryPosition specificLogFilePosition = null;
                    if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                        // 如果指定binlogName +
                        // timestamp，但没有指定对应的offest，尝试根据时间找一下offest
                        EntryPosition endPosition = findEndPosition(damengConnection);
                        if (endPosition != null) {
                            logger.warn("prepare to find start position {}:{}:{}",
                                    new Object[]{entryPosition.getJournalName(), "", entryPosition.getTimestamp()});
                            specificLogFilePosition = findAsPerTimestampInSpecificLogFile(damengConnection,
                                    entryPosition.getTimestamp(),
                                    endPosition,
                                    entryPosition.getJournalName(),
                                    true);
                        }
                    }

                    if (specificLogFilePosition == null) {
                        if (isRdsOssMode()) {
                            // 如果binlog位点不存在，并且属于timestamp不为空,可以返回null走到oss binlog处理
                            return null;
                        }
                        // position不存在，从文件头开始
                        entryPosition.setPosition(BINLOG_START_OFFEST);
                        return entryPosition;
                    } else {
                        return specificLogFilePosition;
                    }
                }
            }
        } else {
            if (logPosition.getIdentity().getSourceAddress().equals(damengConnection.getConnector().getAddress())) {
                if (dumpErrorCountThreshold >= 0 && dumpErrorCount > dumpErrorCountThreshold) {
                    // binlog定位位点失败,可能有两个原因:
                    // 1. binlog位点被删除
                    // 2.vip模式的mysql,发生了主备切换,判断一下serverId是否变化,针对这种模式可以发起一次基于时间戳查找合适的binlog位点
                    boolean case2 = (standbyInfo == null || standbyInfo.getAddress() == null)
                            && logPosition.getPostion().getServerId() != null
                            && !logPosition.getPostion().getServerId().equals(findServerId(damengConnection));
                    if (case2) {
                        EntryPosition findPosition = fallbackFindByStartTimestamp(logPosition, damengConnection);
                        return findPosition;
                    }
                    // 处理 binlog 位点被删除的情况，提供自动重置到当前位点的功能
                    // 应用场景: 测试环境不稳定，位点经常被删。强烈不建议在正式环境中开启此控制参数，因为binlog
                    // 丢失调到最新位点也即意味着数据丢失
                    if (isAutoResetLatestPosMode()) {
                        dumpErrorCount = 0;
                        return findEndPosition(damengConnection);
                    }
                    Long timestamp = logPosition.getPostion().getTimestamp();
                    if (isRdsOssMode() && (timestamp != null && timestamp > 0)) {
                        // 如果binlog位点不存在，并且属于timestamp不为空,可以返回null走到oss binlog处理
                        return null;
                    }
                } else if (StringUtils.isBlank(logPosition.getPostion().getJournalName())
                        && logPosition.getPostion().getPosition() <= 0
                        && logPosition.getPostion().getTimestamp() > 0) {
                    return fallbackFindByStartTimestamp(logPosition, damengConnection);
                }
                // 其余情况
                logger.warn("prepare to find start position just last position\n {}",
                        JsonUtils.marshalToString(logPosition));
                return logPosition.getPostion();
            } else {
                // 针对切换的情况，考虑回退时间
                long newStartTimestamp = logPosition.getPostion().getTimestamp() - fallbackIntervalInSeconds * 1000;
                logger.warn("prepare to find start position by switch {}:{}:{}", new Object[]{"", "",
                        logPosition.getPostion().getTimestamp()});
                return findByStartTimeStamp(damengConnection, newStartTimestamp);
            }
        }
    }

    /**
     * find position by timestamp with a fallback interval seconds.
     *
     * @param logPosition
     * @param mysqlConnection
     * @return
     */
    protected EntryPosition fallbackFindByStartTimestamp(LogPosition logPosition, DamengConnection mysqlConnection) {
        long timestamp = logPosition.getPostion().getTimestamp();
        long newStartTimestamp = timestamp - fallbackIntervalInSeconds * 1000;
        logger.warn("prepare to find start position by last position {}:{}:{}", new Object[]{"", "",
                logPosition.getPostion().getTimestamp()});
        return findByStartTimeStamp(mysqlConnection, newStartTimestamp);
    }

    // 根据想要的position，可能这个position对应的记录为rowdata，需要找到事务头，避免丢数据
    // 主要考虑一个事务执行时间可能会几秒种，如果仅仅按照timestamp相同，则可能会丢失事务的前半部分数据
    private Long findTransactionBeginPosition(ErosaConnection damengConnection, final EntryPosition entryPosition)
            throws IOException {
        // 针对开始的第一条为非Begin记录，需要从该binlog扫描
        final AtomicLong preTransactionStartPosition = new AtomicLong(0L);
        damengConnection.reconnect();
        damengConnection.seek(entryPosition.getJournalName(), Scn.valueOf(entryPosition.getPosition()), new SinkFunction<RedoLog>() {

            private LogPosition lastPosition;

            public boolean sink(RedoLog event) {
                try {
                    CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, true);
                    if (entry == null) {
                        return true;
                    }

                    // 直接查询第一条业务数据，确认是否为事务Begin
                    // 记录一下transaction begin position
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                            && entry.getHeader().getLogfileOffset() < entryPosition.getPosition()) {
                        preTransactionStartPosition.set(entry.getHeader().getLogfileOffset());
                    }

                    if (entry.getHeader().getLogfileOffset() >= entryPosition.getPosition()) {
                        return false;// 退出
                    }

                    lastPosition = buildLastPosition(entry);
                } catch (Exception e) {
                    processSinkError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    return false;
                }

                return running;
            }
        });

        // 判断一下找到的最接近position的事务头的位置
        if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
            logger.error("preTransactionEndPosition greater than startPosition from zk or localconf, maybe lost data");
            throw new CanalParseException("preTransactionStartPosition greater than startPosition from zk or localconf, maybe lost data");
        }
        return preTransactionStartPosition.get();
    }

    // 根据时间查找binlog位置
    private EntryPosition findByStartTimeStamp(DamengConnection damengConnection, Long startTimestamp) {
        EntryPosition endPosition = findEndPosition(damengConnection);
        EntryPosition startPosition = findStartPosition(damengConnection);
        String maxBinlogFileName = endPosition.getJournalName();
        String minBinlogFileName = startPosition.getJournalName();
        logger.info("show master status to set search end condition:{} ", endPosition);
        String startSearchBinlogFile = endPosition.getJournalName();
        boolean shouldBreak = false;
        while (running && !shouldBreak) {
            try {
                EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(damengConnection,
                        startTimestamp,
                        endPosition,
                        startSearchBinlogFile,
                        false);
                if (entryPosition == null) {
                    if (StringUtils.equalsIgnoreCase(minBinlogFileName, startSearchBinlogFile)) {
                        // 已经找到最早的一个binlog，没必要往前找了
                        shouldBreak = true;
                        logger.warn("Didn't find the corresponding binlog files from {} to {}",
                                minBinlogFileName,
                                maxBinlogFileName);
                    } else {
                        // 继续往前找
                        int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                        if (binlogSeqNum <= 1) {
                            logger.warn("Didn't find the corresponding binlog files");
                            shouldBreak = true;
                        } else {
                            int nextBinlogSeqNum = binlogSeqNum - 1;
                            String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                                    startSearchBinlogFile.indexOf(".") + 1);
                            String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                            startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                        }
                    }
                } else {
                    logger.info("found and return:{} in findByStartTimeStamp operation.", entryPosition);
                    return entryPosition;
                }
            } catch (Exception e) {
                logger.warn(String.format("the binlogfile:%s doesn't exist, to continue to search the next binlogfile , caused by",
                                startSearchBinlogFile),
                        e);
                int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    logger.warn("Didn't find the corresponding binlog files");
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                            startSearchBinlogFile.indexOf(".") + 1);
                    String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                    startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                }
            }
        }
        // 找不到
        return null;
    }


//    /**
//     * 查询当前的binlog位置
//     */
//    private EntryPosition findEndPosition(DamengConnection damengConnection) {
//        try {
//            String showSql = multiStreamEnable ? "show master status with " + destination : "show master status";
//            ResultSetPacket packet = damengConnection.query(showSql);
//            List<String> fields = packet.getFieldValues();
//            if (CollectionUtils.isEmpty(fields)) {
//                throw new CanalParseException(
//                        "command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
//            }
//            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
//            if (isGTIDMode() && fields.size() > 4) {
//                endPosition.setGtid(fields.get(4));
//            }
//            // MariaDB 无法通过`show master status`获取 gtid
//            if (damengConnection.isMariaDB() && isGTIDMode()) {
//                ResultSetPacket gtidPacket = damengConnection.query("SELECT @@global.gtid_binlog_pos");
//                List<String> gtidFields = gtidPacket.getFieldValues();
//                if (!CollectionUtils.isEmpty(gtidFields) && gtidFields.size() > 0) {
//                    endPosition.setGtid(gtidFields.get(0));
//                }
//            }
//            return endPosition;
//        } catch (IOException e) {
//            throw new CanalParseException("command : 'show master status' has an error!", e);
//        }
//    }

//    /**
//     * 查询当前的binlog位置
//     */
//    private EntryPosition findStartPosition(DamengConnection damengConnection) {
//        try {
//            String showSql = multiStreamEnable ?
//                    "show binlog events with " + destination + " limit 1" : "show binlog events limit 1";
//            ResultSetPacket packet = damengConnection.query(showSql);
//            List<String> fields = packet.getFieldValues();
//            if (CollectionUtils.isEmpty(fields)) {
//                throw new CanalParseException(
//                        "command : 'show binlog events limit 1' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
//            }
//            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
//            return endPosition;
//        } catch (IOException e) {
//            throw new CanalParseException("command : 'show binlog events limit 1' has an error!", e);
//        }
//
//    }

//    /**
//     * 查询当前的slave视图的binlog位置
//     */
//    @SuppressWarnings("unused")
//    private SlaveEntryPosition findSlavePosition(DamengConnection damengConnection) {
//        try {
//            ResultSetPacket packet = damengConnection.query("show slave status");
//            List<FieldPacket> names = packet.getFieldDescriptors();
//            List<String> fields = packet.getFieldValues();
//            if (CollectionUtils.isEmpty(fields)) {
//                return null;
//            }
//
//            int i = 0;
//            Map<String, String> maps = new HashMap<>(names.size(), 1f);
//            for (FieldPacket name : names) {
//                maps.put(name.getName(), fields.get(i));
//                i++;
//            }
//
//            String errno = maps.get("Last_Errno");
//            String slaveIORunning = maps.get("Slave_IO_Running"); // Slave_SQL_Running
//            String slaveSQLRunning = maps.get("Slave_SQL_Running"); // Slave_SQL_Running
//            if ((!"0".equals(errno)) || (!"Yes".equalsIgnoreCase(slaveIORunning))
//                    || (!"Yes".equalsIgnoreCase(slaveSQLRunning))) {
//                logger.warn("Ignoring failed slave: " + damengConnection.getConnector().getAddress() + ", Last_Errno = "
//                        + errno + ", Slave_IO_Running = " + slaveIORunning + ", Slave_SQL_Running = "
//                        + slaveSQLRunning);
//                return null;
//            }
//
//            String masterHost = maps.get("Master_Host");
//            String masterPort = maps.get("Master_Port");
//            String binlog = maps.get("Master_Log_File");
//            String position = maps.get("Exec_Master_Log_Pos");
//            return new SlaveEntryPosition(binlog, Long.valueOf(position), masterHost, masterPort);
//        } catch (IOException e) {
//            logger.error("find slave position error", e);
//        }
//
//        return null;
//    }

    /**
     * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。
     * 针对最后一个binlog会给定endPosition，避免无尽的查询
     */
    private EntryPosition findAsPerTimestampInSpecificLogFile(DamengConnection damengConnection,
                                                              final Long startTimestamp,
                                                              final EntryPosition endPosition,
                                                              final String searchBinlogFile,
                                                              final Boolean justForPositionTimestamp) {

        final LogPosition logPosition = new LogPosition();
        try {
            damengConnection.reconnect();
            // 开始遍历文件
            damengConnection.seek(searchBinlogFile, 4L, endPosition.getGtid(), new SinkFunction<LogEvent>() {

                private LogPosition lastPosition;

                public boolean sink(LogEvent event) {
                    EntryPosition entryPosition = null;
                    try {
                        CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, true);
                        if (justForPositionTimestamp && logPosition.getPostion() == null && event.getWhen() > 0) {
                            // 初始位点
                            entryPosition = new EntryPosition(searchBinlogFile,
                                    event.getLogPos() - event.getEventLen(),
                                    event.getWhen() * 1000,
                                    event.getServerId());
                            entryPosition.setGtid(event.getHeader().getGtidSetStr());
                            logPosition.setPostion(entryPosition);
                        }

                        // 直接用event的位点来处理,解决一个binlog文件里没有任何事件导致死循环无法退出的问题
                        String logfilename = event.getHeader().getLogFileName();
                        // 记录的是binlog end offest,
                        // 因为与其对比的offest是show master status里的end offest
                        Long logfileoffset = event.getHeader().getLogPos();
                        Long logposTimestamp = event.getHeader().getWhen() * 1000;
                        Long serverId = event.getHeader().getServerId();

                        // 如果最小的一条记录都不满足条件，可直接退出
                        if (logposTimestamp >= startTimestamp) {
                            return false;
                        }

                        if (StringUtils.equals(endPosition.getJournalName(), logfilename)
                                && endPosition.getPosition() <= logfileoffset) {
                            return false;
                        }

                        if (entry == null) {
                            return true;
                        }

                        // 记录一下上一个事务结束的位置，即下一个事务的position
                        // position = current +
                        // data.length，代表该事务的下一条offest，避免多余的事务重复
                        if (CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp, serverId);
                            if (logger.isDebugEnabled()) {
                                logger.debug("set {} to be pending start position before finding another proper one...",
                                        entryPosition);
                            }
                            logPosition.setPostion(entryPosition);
                            entryPosition.setGtid(entry.getHeader().getGtid());
                        } else if (CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())) {
                            // 当前事务开始位点
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp, serverId);
                            if (logger.isDebugEnabled()) {
                                logger.debug("set {} to be pending start position before finding another proper one...",
                                        entryPosition);
                            }
                            entryPosition.setGtid(entry.getHeader().getGtid());
                            logPosition.setPostion(entryPosition);
                        }

                        lastPosition = buildLastPosition(entry);
                    } catch (Throwable e) {
                        processSinkError(e, lastPosition, searchBinlogFile, 4L);
                    }

                    return running;
                }
            });

        } catch (IOException e) {
            logger.error("ERROR ## findAsPerTimestampInSpecificLogFile has an error", e);
        }

        if (logPosition.getPostion() != null) {
            return logPosition.getPostion();
        } else {
            return null;
        }
    }


    // ===================== setter / getter ========================

    public void setDefaultConnectionTimeoutInSeconds(int defaultConnectionTimeoutInSeconds) {
        this.defaultConnectionTimeoutInSeconds = defaultConnectionTimeoutInSeconds;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public void setMasterInfo(AuthenticationInfo masterInfo) {
        this.masterInfo = masterInfo;
    }

    public void setStandbyInfo(AuthenticationInfo standbyInfo) {
        this.standbyInfo = standbyInfo;
    }

    public void setMasterPosition(EntryPosition masterPosition) {
        this.masterPosition = masterPosition;
    }

    public void setStandbyPosition(EntryPosition standbyPosition) {
        this.standbyPosition = standbyPosition;
    }

    public void setDetectingSQL(String detectingSQL) {
        this.detectingSQL = detectingSQL;
    }

    public void setDetectingIntervalInSeconds(Integer detectingIntervalInSeconds) {
        this.detectingIntervalInSeconds = detectingIntervalInSeconds;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public void setFallbackIntervalInSeconds(int fallbackIntervalInSeconds) {
        this.fallbackIntervalInSeconds = fallbackIntervalInSeconds;
    }

    public CanalHAController getHaController() {
        return haController;
    }

    public void setHaController(CanalHAController haController) {
        this.haController = haController;
    }

    public boolean isRdsOssMode() {
        return rdsOssMode;
    }

    public void setRdsOssMode(boolean rdsOssMode) {
        this.rdsOssMode = rdsOssMode;
    }

    public boolean isAutoResetLatestPosMode() {
        return autoResetLatestPosMode;
    }

    public void setAutoResetLatestPosMode(boolean autoResetLatestPosMode) {
        this.autoResetLatestPosMode = autoResetLatestPosMode;
    }

    public void setMultiStreamEnable(boolean multiStreamEnable) {
        this.multiStreamEnable = multiStreamEnable;
    }
}
