/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.dbsync.logmnr.event.dameng;

import com.eshore.dbsync.logmnr.Scn;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.*;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static io.debezium.connector.dameng.logminer.LogMinerHelper.*;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final DamengConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final DamengDatabaseSchema schema;
    private final DamengOffsetContext offsetContext;
    private final boolean isRac;
    private final Set<String> racHosts = new HashSet<>();
    private final JdbcConfiguration jdbcConfiguration;
    private final DamengConnectorConfig.LogMiningStrategy strategy;
    private final DamengTaskContext taskContext;
    private final ErrorHandler errorHandler;
    private final boolean isContinuousMining;
    private boolean isContinuousMining1;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;
    private final DamengConnectorConfig connectorConfig;
    private final Duration archiveLogRetention;

    private Scn startScn;
    private Scn endScn;
    private List<BigInteger> currentRedoLogSequences;

    public LogMinerStreamingChangeEventSource(DamengConnectorConfig connectorConfig, DamengOffsetContext offsetContext,
                                              DamengConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, DamengDatabaseSchema schema,
                                              DamengTaskContext taskContext, Configuration jdbcConfig,
                                              DamengStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.connectorConfig = connectorConfig;
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.isContinuousMining1 = false;
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.isRac = connectorConfig.isRacSystem();
        if (this.isRac) {
            this.racHosts.addAll(connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet()));
            instantiateFlushConnections(jdbcConfiguration, racHosts);
        }
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context) {
        try (TransactionalBuffer transactionalBuffer = new TransactionalBuffer(schema, clock, errorHandler, streamingMetrics)) {
            try {
                startScn = offsetContext.getScn();
                createFlushTable(jdbcConnection);

                if (!isContinuousMining && startScn.compareTo(getFirstOnlineLogScn(jdbcConnection, archiveLogRetention)) < 0) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn + ".  Please perform a new snapshot.");
                }

                // setNlsSessionParameters(jdbcConnection);
                // checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

                // initializeRedoLogsForMining改了
                initializeRedoLogsForMining(jdbcConnection, false, archiveLogRetention);

                HistoryRecorder historyRecorder = connectorConfig.getLogMiningHistoryRecorder();

                try {
                    // todo: why can't OracleConnection be used rather than a Factory+JdbcConfiguration?
                    historyRecorder.prepare(streamingMetrics, jdbcConfiguration, connectorConfig.getLogMinerHistoryRetentionHours());

                    final LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context, jdbcConnection,
                            connectorConfig, streamingMetrics, transactionalBuffer, offsetContext, schema, dispatcher,
                            clock, historyRecorder);

                    // final String query = SqlUtils.logMinerContentsQuery(connectorConfig, jdbcConnection.username());
                    final String query1 = "SELECT * FROM V$LOGMNR_CONTENTS WHERE SCN > ? AND SCN <= ? AND ((OPERATION_CODE  IN (5, 34) AND SEG_OWNER NOT IN ('SYS', 'SYSTEM', 'SYSDBA')) OR (OPERATION_CODE IN (7, 36)) OR (OPERATION_CODE IN (1, 2, 3) AND TABLE_NAME != 'LOG_MINING_FLUSH' AND SEG_OWNER NOT IN ('APPQOSSYS', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'GSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'OUTLN', 'SYS', 'SYSTEM', 'WMSYS', 'XDB') )) ORDER BY SCN;";
                    try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query1, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {

                        // getCurrentRedoLogSequences()改了
                        currentRedoLogSequences = getCurrentRedoLogSequences();

                        Stopwatch stopwatch = Stopwatch.reusable();
                        while (context.isRunning()) {

                            // getSystime(jdbcConnection)改了
                            streamingMetrics.calculateTimeDifference(getSystime(jdbcConnection));

                            Instant start = Instant.now();
                            endScn = getEndScn(jdbcConnection, startScn, streamingMetrics, connectorConfig.getLogMiningBatchSizeDefault());
                            flushLogWriter(jdbcConnection, jdbcConfiguration, isRac, racHosts);

                            if (hasLogSwitchOccurred()) {
                                LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                        startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                                endMining(jdbcConnection);

                                initializeRedoLogsForMining(jdbcConnection, true, archiveLogRetention);

                                abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);

                                currentRedoLogSequences = getCurrentRedoLogSequences();
                            }
                            else {
                                if (isContinuousMining1) {
                                    // endMining(jdbcConnection);
                                    // LogMinerHelper.setDamengRedoLogFilesForMining(jdbcConnection, startScn, null);
                                    // abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);
                                    // currentRedoLogSequences = getCurrentRedoLogSequences();
                                    LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                                            startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
                                    endMining(jdbcConnection);

                                    initializeRedoLogsForMining(jdbcConnection, true, archiveLogRetention);

                                    abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);

                                    currentRedoLogSequences = getCurrentRedoLogSequences();
                                }
                            }
                            // startLogMining改了
                            startLogMining(jdbcConnection, startScn, endScn, strategy, isContinuousMining, streamingMetrics);

                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setString(1, startScn.toString());
                            miningView.setString(2, endScn.toString());
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing = stopwatch.stop().durations().statistics().getTotal();
                                streamingMetrics.setLastDurationOfBatchCapturing(lastDurationOfBatchCapturing);
                                processor.processResult(rs);

                                startScn = endScn;

                                if (transactionalBuffer.isEmpty()) {
                                    LOGGER.debug("Transactional buffer empty, updating offset's SCN {}", startScn);
                                    offsetContext.setScn(startScn);
                                }
                            }

                            streamingMetrics.setCurrentBatchProcessingTime(Duration.between(start, Instant.now()));
                            pauseBetweenMiningSessions();
                            isContinuousMining1 = true;
                        }
                    }
                }
                finally {
                    historyRecorder.close();
                }
            }
            catch (Throwable t) {
                logError(streamingMetrics, "Mining session stopped due to the {}", t);
                errorHandler.setProducerThrowable(t);
            }
            finally {
                LOGGER.info("startScn={}, endScn={}, offsetContext.getScn()={}", startScn, endScn, offsetContext.getScn());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.toString());
                LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            }
        }
    }

    private void abandonOldTransactionsIfExist(DamengConnection connection, TransactionalBuffer transactionalBuffer) {
        Duration transactionRetention = connectorConfig.getLogMiningTransactionRetention();
        if (!Duration.ZERO.equals(transactionRetention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(connection, offsetScn, transactionRetention);
            lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
                transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
                offsetContext.setScn(thresholdScn);
                startScn = endScn;
            });
        }
    }

    private void initializeRedoLogsForMining(DamengConnection connection, boolean postEndMiningSession, Duration archiveLogRetention) throws SQLException {
        if (!postEndMiningSession) {
            if (DamengConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                // buildDataDictionary(connection);
            }
            if (!isContinuousMining) {
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        }
        else {
            if (!isContinuousMining) {
                if (DamengConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    // buildDataDictionary(connection);
                }
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        }
    }

    /**
     * Checks whether a database log switch has occurred and updates metrics if so.
     *
     * @return {@code true} if a log switch was detected, otherwise {@code false}
     * @throws SQLException if a database exception occurred
     */
    private boolean hasLogSwitchOccurred() throws SQLException {
        final List<BigInteger> newSequences = getCurrentRedoLogSequences();
        if (!newSequences.equals(currentRedoLogSequences)) {
            LOGGER.debug("Current log sequence(s) is now {}, was {}", newSequences, currentRedoLogSequences);

            currentRedoLogSequences = newSequences;

            // final Map<String, String> logStatuses = jdbcConnection.queryAndMap(SqlUtils.redoLogStatusQuery(), rs -> {
            final Map<String, String> logStatuses = jdbcConnection.queryAndMap("select PATH, STATUS from V$ARCH_FILE", rs -> {
                Map<String, String> results = new LinkedHashMap<>();
                while (rs.next()) {
                    results.put(rs.getString(1), rs.getString(2));
                }
                return results;
            });

            final int logSwitchCount = jdbcConnection.queryAndMap("SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG", rs -> {
                // final int logSwitchCount = jdbcConnection.queryAndMap(SqlUtils.switchHistoryQuery(), rs -> {
                if (rs.next()) {
                    return rs.getInt(2);
                }
                return 0;
            });

            final Set<String> fileNames = getCurrentRedoLogFiles(jdbcConnection);

            streamingMetrics.setRedoLogStatus(logStatuses);
            streamingMetrics.setSwitchCount(logSwitchCount);
            streamingMetrics.setCurrentLogFileName(fileNames);

            return true;
        }

        return false;
    }

    /**
     * Get the current redo log sequence(s).
     * <p>
     * In an Oracle RAC environment, there are multiple current redo logs and therefore this method
     * returns multiple values, each relating to a single RAC node in the Oracle cluster.
     *
     * @return list of sequence numbers
     * @throws SQLException if a database exception occurred
     */
    private List<BigInteger> getCurrentRedoLogSequences() throws SQLException {

        return jdbcConnection.queryAndMap("select SEQUENCE# from V$ARCH_FILE F,V$ARCHIVED_LOG L WHERE  F.PATH = L.NAME AND F.STATUS = 'ACTIVE'", rs -> {
            // return jdbcConnection.queryAndMap(SqlUtils.currentRedoLogSequenceQuery(), rs -> {
            List<BigInteger> sequences = new ArrayList<>();
            while (rs.next()) {
                sequences.add(new BigInteger(rs.getString(1)));
            }
            return sequences;
        });
    }

    private void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period = Duration.ofMillis(streamingMetrics.getMillisecondToSleepBetweenMiningQuery());
        Metronome.sleeper(period, clock).pause();
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
