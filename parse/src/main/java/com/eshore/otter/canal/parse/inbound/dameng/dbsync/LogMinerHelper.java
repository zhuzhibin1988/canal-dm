/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.otter.canal.parse.inbound.dameng.dbsync;

import com.eshore.dbsync.logminer.Scn;
import com.eshore.dbsync.logminer.event.dameng.LogFile;
import com.eshore.dbsync.logminer.event.dameng.Scn;
import com.eshore.otter.canal.parse.driver.dameng.DamengConnector;
import com.eshore.otter.canal.parse.inbound.dameng.DamengConnection;
import io.debezium.DebeziumException;
import io.debezium.connector.dameng.*;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class contains methods to configure and manage LogMiner utility
 */
public class LogMinerHelper {

    private static final String CURRENT = "CURRENT";
    private static final String UNKNOWN = "unknown";
    private static final String TOTAL = "TOTAL";
    private static final String ALL_COLUMN_LOGGING = "ALL COLUMN LOGGING";
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    public enum DATATYPE {
        LONG,
        TIMESTAMP,
        STRING,
        FLOAT
    }

    private static Map<String, DamengConnection> racFlushConnections = new HashMap<>();

    static void instantiateFlushConnections(JdbcConfiguration config, Set<String> hosts) {
        for (DamengConnection conn : racFlushConnections.values()) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.warn("Cannot close existing RAC flush connection", e);
                }
            }
        }
        racFlushConnections = new HashMap<>();
        for (String host : hosts) {
            try {
                racFlushConnections.put(host, createFlushConnection(config, host));
            } catch (SQLException e) {
                LOGGER.error("Cannot connect to RAC node {}", host, e);
            }
        }
    }

    /**
     * This builds data dictionary objects in redo log files.
     * During this build, Oracle does an additional REDO LOG switch.
     * This call may take time, which leads to delay in delivering incremental changes.
     * With this option the lag between source database and dispatching event fluctuates.
     *
     * @param connection connection to the database as LogMiner user (connection to the container)
     * @throws SQLException any exception
     */
    static void buildDataDictionary(DamengConnection connection) throws SQLException {
        LOGGER.trace("Building data dictionary");
        executeCallableStatement(connection, LogMinerSqls.BUILD_DICTIONARY);
    }

    /**
     * This method returns current SCN from the database
     *
     * @param connection container level database connection
     * @return current SCN
     * @throws SQLException if anything unexpected happens
     */
    public static Scn getCurrentScn(DamengConnection connection) throws SQLException {
        try (Statement statement = connection.connection(false).createStatement();
             // ResultSet rs = statement.executeQuery(LogMinerSqls.currentScnQuery())) {
             ResultSet rs = statement.executeQuery("SELECT CLSN FROM V$ARCH_FILE WHERE STATUS = 'ACTIVE'")) {
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            return Scn.valueOf(rs.getString(1));
        }
    }

    static void createFlushTable(DamengConnection connection) throws SQLException {
        String tableExists = (String) getSingleResult(connection, LogMinerSqls.tableExistsQuery(LogMinerSqls.LOGMNR_FLUSH_TABLE), DATATYPE.STRING);
        if (tableExists == null) {
            executeCallableStatement(connection, LogMinerSqls.CREATE_FLUSH_TABLE);
        }

        String recordExists = (String) getSingleResult(connection, LogMinerSqls.FLUSH_TABLE_NOT_EMPTY, DATATYPE.STRING);
        if (recordExists == null) {
            executeCallableStatement(connection, LogMinerSqls.INSERT_FLUSH_TABLE);
            connection.commit();
        }
    }

    /**
     * This method returns next SCN for mining and also updates streaming metrics.
     * <p>
     * We use a configurable limit, because the larger mining range, the slower query from LogMiner content view.
     * In addition capturing unlimited number of changes can blow up Java heap.
     * Gradual querying helps to catch up faster after long delays in mining.
     *
     * @param connection       container level database connection
     * @param startScn         start SCN
     * @param streamingMetrics the streaming metrics
     * @return next SCN to mine up to
     * @throws SQLException if anything unexpected happens
     */
    static Scn getEndScn(DamengConnection connection, Scn startScn, DamengStreamingChangeEventSourceMetrics streamingMetrics, int defaultBatchSize) throws SQLException {
        Scn currentScn = getCurrentScn(connection);
        streamingMetrics.setCurrentScn(currentScn);

        Scn topScnToMine = startScn.add(Scn.valueOf(streamingMetrics.getBatchSize()));

        // adjust batch size
        boolean topMiningScnInFarFuture = false;
        if (topScnToMine.subtract(currentScn).compareTo(Scn.valueOf(defaultBatchSize)) > 0) {
            streamingMetrics.changeBatchSize(false);
            topMiningScnInFarFuture = true;
        }
        if (currentScn.subtract(topScnToMine).compareTo(Scn.valueOf(defaultBatchSize)) > 0) {
            streamingMetrics.changeBatchSize(true);
        }

        // adjust sleeping time to reduce DB impact
        if (currentScn.compareTo(topScnToMine) < 0) {
            if (!topMiningScnInFarFuture) {
                streamingMetrics.changeSleepingTime(true);
            }
            return currentScn;
        } else {
            streamingMetrics.changeSleepingTime(false);
            return topScnToMine;
        }
    }

    /**
     * It is critical to flush LogWriter(s) buffer
     *
     * @param connection container level database connection
     * @param config     configuration
     * @param isRac      true if this is the RAC system
     * @param racHosts   set of RAC host
     * @throws SQLException exception
     */
    static void flushLogWriter(DamengConnection connection, JdbcConfiguration config,
                               boolean isRac, Set<String> racHosts)
            throws SQLException {
        Scn currentScn = getCurrentScn(connection);
        if (isRac) {
            flushRacLogWriters(currentScn, config, racHosts);
        } else {
            LOGGER.trace("Updating {} with SCN {}", LogMinerSqls.LOGMNR_FLUSH_TABLE, currentScn);
            executeCallableStatement(connection, LogMinerSqls.UPDATE_FLUSH_TABLE + currentScn);
            connection.commit();
        }
    }

    /**
     * Get the database time in the time zone of the system this database is running on
     *
     * @param connection connection
     * @return the database system time
     */
    static OffsetDateTime getSystime(DamengConnection connection) throws SQLException {
        return connection.queryAndMap(LogMinerSqls.SELECT_SYSTIMESTAMP, rs -> {
            if (rs.next()) {
                long timeStamp2 = rs.getTimestamp(1).getTime();
                LocalDateTime localDateTime2 = Instant.ofEpochSecond(timeStamp2).atOffset(ZoneOffset.UTC).toLocalDateTime();
                OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime2, ZoneOffset.UTC);
                return offsetDateTime;
                // return rs.getObject(1, OffsetDateTime.class);
            } else {
                return null;
            }
        });
    }

    /**
     * This method builds mining view to query changes from.
     * This view is built for online redo log files.
     * It starts log mining session.
     * It uses data dictionary objects, incorporated in previous steps.
     * It tracks DDL changes and mines committed data only.
     *
     * @param connection         container level database connection
     * @param startScn           the SCN to mine from
     * @param endScn             the SCN to mine to
     * @param strategy           this is about dictionary location
     * @param isContinuousMining works < 19 version only
     * @param streamingMetrics   the streaming metrics
     * @throws SQLException if anything unexpected happens
     */
    static void startLogMining(DamengConnection connection, Scn startScn, Scn endScn,
                               DamengConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining, DamengStreamingChangeEventSourceMetrics streamingMetrics)
            throws SQLException {
        LOGGER.trace("Starting log mining startScn={}, endScn={}, strategy={}, continuous={}", startScn, endScn, strategy, isContinuousMining);

        String statement = LogMinerSqls.startLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
        try {
            Instant start = Instant.now();
            executeCallableStatement(connection, statement);
            streamingMetrics.addCurrentMiningSessionStart(Duration.between(start, Instant.now()));
        } catch (SQLException e) {
            // Capture database state before throwing exception
            logDatabaseState(connection);
            throw e;
        }
        // todo dbms_logmnr.STRING_LITERALS_IN_STMT?
        // todo If the log file is corrupted/bad, logmnr will not be able to access it, we have to switch to another one?
    }

    /**
     * This method query the database to get CURRENT online redo log file(s). Multiple is applicable for RAC systems.
     *
     * @param connection connection to reuse
     * @return full redo log file name(s), including path
     * @throws SQLException if anything unexpected happens
     */
    static Set<String> getCurrentRedoLogFiles(DamengConnection connection) throws SQLException {
        final Set<String> fileNames = new HashSet<>();

        connection.query("SELECT PATH FROM V$ARCH_FILE WHERE STATUS = 'ACTIVE'", rs -> {
            // connection.query(LogMinerSqls.currentRedoNameQuery(), rs -> {
            while (rs.next()) {
                fileNames.add(rs.getString(1));
            }
        });
        LOGGER.trace(" Current Redo log fileNames: {} ", fileNames);
        return fileNames;
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection          container level database connection
     * @param archiveLogRetention duration that archive logs are mined
     * @return oldest SCN from online redo log
     * @throws SQLException if anything unexpected happens
     */
    static Scn getFirstOnlineLogScn(DamengConnection connection, Duration archiveLogRetention) throws SQLException {
        LOGGER.trace("Getting first scn of all online logs");
        try (Statement s = connection.connection(false).createStatement()) {
            try (ResultSet rs = s.executeQuery(LogMinerSqls.oldestFirstChangeQuery(archiveLogRetention))) {
                rs.next();
                Scn firstScnOfOnlineLog = Scn.valueOf(rs.getString(1));
                LOGGER.trace("First SCN in online logs is {}", firstScnOfOnlineLog);
                return firstScnOfOnlineLog;
            }
        }
    }

    /**
     * Sets NLS parameters for mining session.
     *
     * @param connection session level database connection
     * @throws SQLException if anything unexpected happens
     */
    static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(LogMinerSqls.NLS_SESSION_PARAMETERS);
        // This is necessary so that TIMESTAMP WITH LOCAL TIME ZONE get returned in UTC
        connection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");
    }

    /**
     * This fetches online redo log statuses
     *
     * @param connection privileged connection
     * @return REDO LOG statuses Map, where key is the REDO name and value is the status
     * @throws SQLException if anything unexpected happens
     */
    private static Map<String, String> getRedoLogStatus(DamengConnection connection) throws SQLException {
        return getMap(connection, LogMinerSqls.redoLogStatusQuery(), UNKNOWN);
    }

    /**
     * This fetches REDO LOG switch count for the last day
     *
     * @param connection privileged connection
     * @return counter
     */
    private static int getSwitchCount(DamengConnection connection) {
        try {
            Map<String, String> total = getMap(connection, LogMinerSqls.switchHistoryQuery(), UNKNOWN);
            if (total != null && total.get(TOTAL) != null) {
                return Integer.parseInt(total.get(TOTAL));
            }
        } catch (Exception e) {
            LOGGER.error("Cannot get switch counter", e);
        }
        return 0;
    }

    /**
     * Oracle RAC has one LogWriter per node (instance), we have to flush them all
     * We cannot use a query like from gv_instance view to get all the nodes, because not all nodes could be load balanced.
     * We also cannot rely on connection factory, because it may return connection to the same instance multiple times
     * Instead we are asking node ip list from configuration
     */
    private static void flushRacLogWriters(Scn currentScn, JdbcConfiguration config, Set<String> racHosts) {
        Instant startTime = Instant.now();
        if (racHosts.isEmpty()) {
            throw new RuntimeException("No RAC node ip addresses were supplied in the configuration");
        }

        // todo: Ugly, but, using one factory.connect() in the loop, it may always connect the same node with badly configured load balancer
        boolean errors = false;
        for (String host : racHosts) {
            try {
                DamengConnection conn = racFlushConnections.get(host);
                if (conn == null) {
                    LOGGER.warn("Connection to the node {} was not instantiated", host);
                    errors = true;
                    continue;
                }
                LOGGER.trace("Flushing Log Writer buffer of node {}", host);
                executeCallableStatement(conn, LogMinerSqls.UPDATE_FLUSH_TABLE + currentScn);
                conn.commit();
            } catch (Exception e) {
                LOGGER.warn("Cannot flush Log Writer buffer of the node {} due to {}", host, e);
                errors = true;
            }
        }

        if (errors) {
            instantiateFlushConnections(config, racHosts);
            LOGGER.warn("Not all LogWriter buffers were flushed. Sleeping for 3 seconds to let Oracle do the flush.", racHosts);
            Metronome metronome = Metronome.sleeper(Duration.ofMillis(3000), Clock.system());
            try {
                metronome.pause();
            } catch (InterruptedException e) {
                LOGGER.warn("Metronome was interrupted");
            }
        }

        LOGGER.trace("Flushing RAC Log Writers took {} ", Duration.between(startTime, Instant.now()));
    }

    // todo use pool
    private static DamengConnection createFlushConnection(JdbcConfiguration config, String host) throws SQLException {
        JdbcConfiguration hostConfig = JdbcConfiguration.adapt(config.edit().with(JdbcConfiguration.DATABASE, host).build());
        DamengConnection connection = new DamengConnection(hostConfig, () -> LogMinerHelper.class.getClassLoader());
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * This method validates the supplemental logging configuration for the source database.
     *
     * @param connection oracle connection on LogMiner level
     * @param pdbName    pdb name
     * @param schema     oracle schema
     * @throws SQLException if anything unexpected happens
     */
    static void checkSupplementalLogging(DamengConnection connection, String pdbName, DamengDatabaseSchema schema) throws SQLException {
        try {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }

            // Check if ALL supplemental logging is enabled at the database
            Map<String, String> globalAll = getMap(connection, LogMinerSqls.databaseSupplementalLoggingAllCheckQuery(), UNKNOWN);
            if ("NO".equalsIgnoreCase(globalAll.get("KEY"))) {
                // Check if MIN supplemental logging is enabled at the database
                Map<String, String> globalMin = getMap(connection, LogMinerSqls.databaseSupplementalLoggingMinCheckQuery(), UNKNOWN);
                if ("NO".equalsIgnoreCase(globalMin.get("KEY"))) {
                    throw new DebeziumException("Supplemental logging not properly configured.  Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
                }

                // If ALL supplemental logging is not enabled, then each monitored table should be set to ALL COLUMNS
                for (TableId tableId : schema.getTables().tableIds()) {
                    if (!isTableSupplementalLogDataAll(connection, tableId)) {
                        throw new DebeziumException("Supplemental logging not configured for table " + tableId + ".  " +
                                "Use command: ALTER TABLE " + tableId.schema() + "." + tableId.table() + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
                    }
                }
            }
        } finally {
            if (pdbName != null) {
                connection.resetSessionToCdb();
            }
        }
    }

    static boolean isTableSupplementalLogDataAll(DamengConnection connection, TableId tableId) throws SQLException {
        return connection.queryAndMap(LogMinerSqls.tableSupplementalLoggingCheckQuery(tableId), (rs) -> {
            while (rs.next()) {
                if (ALL_COLUMN_LOGGING.equals(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * This call completes LogMiner session.
     * Complete gracefully.
     *
     * @param connection container level database connection
     */
    public static void endMining(DamengConnection connection) {
        String stopMining = LogMinerSqls.END_LOGMNR;
        try {
            executeCallableStatement(connection, stopMining);
        } catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("LogMiner session was already closed");
            } else {
                LOGGER.error("Cannot close LogMiner session gracefully: {}", e);
            }
        }
    }

    /**
     * This method substitutes CONTINUOUS_MINE functionality
     *
     * @param connection          connection
     * @param lastProcessedScn    current offset
     * @param archiveLogRetention the duration that archive logs will be mined
     * @throws SQLException if anything unexpected happens
     */
    // todo: check RAC resiliency
    public static void setRedoLogFilesForMining(DamengConnection connection, Scn lastProcessedScn, Duration archiveLogRetention) throws SQLException {

        removeLogFilesFromMining(connection);

        List<LogFile> onlineLogFilesForMining = getOnlineLogFilesForOffsetScn(connection, lastProcessedScn);
        List<LogFile> archivedLogFilesForMining = getArchivedLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention);

        final boolean hasOverlappingLogFile = onlineLogFilesForMining.stream().anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0)
                || archivedLogFilesForMining.stream().anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0);
        if (!hasOverlappingLogFile) {
            throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        }

        // Deduplicate log files with the same SCn ranges.
        // todo: could this be eliminated by restricting the online log query to those there 'ARCHIVED="NO"'?
        List<String> logFilesNames = archivedLogFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
        for (LogFile redoLog : onlineLogFilesForMining) {
            boolean found = false;
            for (LogFile archiveLog : archivedLogFilesForMining) {
                if (archiveLog.isSameRange(redoLog)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                logFilesNames.add(redoLog.getFileName());
            }
        }

        for (String file : logFilesNames) {
            LOGGER.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = LogMinerSqls.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
        }

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    /**
     * @param connection
     * @param lastProcessedScn
     * @param archiveLogRetention
     * @throws SQLException
     */
    public static void setDamengRedoLogFilesForMining(DamengConnection connection, Scn lastProcessedScn, Duration archiveLogRetention) throws SQLException {
        List<LogFile> onlineLogFilesForMining = getOnlineLogFilesForOffsetScn(connection, lastProcessedScn);
        // List<LogFile> archivedLogFilesForMining = getArchivedLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention);
        //
        // final boolean hasOverlappingLogFile = onlineLogFilesForMining.stream().anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0)
        // || archivedLogFilesForMining.stream().anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0);
        // if (!hasOverlappingLogFile) {
        // throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        // }
        // List<String> logFilesNames = archivedLogFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
        // for (LogFile redoLog : onlineLogFilesForMining) {
        // boolean found = false;
        // for (LogFile archiveLog : archivedLogFilesForMining) {
        // if (archiveLog.isSameRange(redoLog)) {
        // found = true;
        // break;
        // }
        // }
        // if (!found) {
        // logFilesNames.add(redoLog.getFileName());
        // }
        // }

        for (LogFile file : onlineLogFilesForMining) {
            LOGGER.trace("Adding log file {} to mining session", file.getFileName());
            String addLogFileStatement = "BEGIN DBMS_LOGMNR.ADD_LOGFILE('" + file.getFileName() + "') ;END;";
            executeCallableStatement(connection, addLogFileStatement);
        }
        // LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    /**
     * This method calculates SCN as a watermark to abandon long lasting transactions.
     * The criteria is don't let offset scn go out of archives older given number of hours
     *
     * @param connection           connection
     * @param offsetScn            current offset scn
     * @param transactionRetention duration to tolerate long running transactions
     * @return optional SCN as a watermark for abandonment
     */
    public static Optional<Scn> getLastScnToAbandon(DamengConnection connection, Scn offsetScn, Duration transactionRetention) {
        try {
            String query = LogMinerSqls.diffInDaysQuery(offsetScn);
            Float diffInDays = (Float) getSingleResult(connection, query, DATATYPE.FLOAT);
            if (diffInDays != null && (diffInDays * 24) > transactionRetention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        } catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference due to {}", e);
            return Optional.of(offsetScn);
        }
    }

    static void logWarn(DamengStreamingChangeEventSourceMetrics streamingMetrics, String format, Object... args) {
        LOGGER.warn(format, args);
        streamingMetrics.incrementWarningCount();
    }

    static void logError(DamengStreamingChangeEventSourceMetrics streamingMetrics, String format, Object... args) {
        LOGGER.error(format, args);
        streamingMetrics.incrementErrorCount();
    }

    /**
     * This method returns all online log files, starting from one which contains offset SCN and ending with one containing largest SCN
     * 18446744073709551615 on Ora 19c is the max value of the nextScn in the current redo
     */
    public static List<LogFile> getOnlineLogFilesForOffsetScn(DamengConnection connection, Scn offsetScn) throws SQLException {
        LOGGER.trace("Getting online redo logs for offset scn {}", offsetScn);
        List<LogFile> redoLogFiles = new ArrayList<>();

        // try (PreparedStatement s = connection.connection(false).prepareStatement(LogMinerSqls.allOnlineLogsQuery())) {
        String sql = "SELECT PATH,ARCH_LSN,CLSN,STATUS FROM SYS.V$ARCH_FILE ";
        try (PreparedStatement s = connection.connection(false).prepareStatement(sql)) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    Scn firstChangeNumber = getScnFromString(rs.getString(2));
                    Scn nextChangeNumber = getScnFromString(rs.getString(3));
                    String status = rs.getString(4);
                    LogFile logFile = new LogFile(fileName, firstChangeNumber, nextChangeNumber, "ACTIVE".equalsIgnoreCase(status));
                    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) to be added.", fileName, firstChangeNumber, nextChangeNumber, status);
                        redoLogFiles.add(logFile);
                    } else {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) to be excluded.", fileName, firstChangeNumber, nextChangeNumber, status);
                    }
                }
            }
        }
        return redoLogFiles;
    }

    private static Scn getScnFromString(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return Scn.MAX;
        }
        return Scn.valueOf(value);
    }

    /**
     * Helper method that will dump the state of various critical tables used by the LogMiner implementation
     * to derive state about which logs are to be mined and processed by the Oracle LogMiner session.
     *
     * @param connection the database connection
     */
    private static void logDatabaseState(DamengConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            // LOGGER.debug("Configured redo logs are:");
            // try {
            // logQueryResults(connection, "SELECT * FROM V$LOGFILE");
            // }
            // catch (SQLException e) {
            // LOGGER.debug("Failed to obtain redo log table entries", e);
            // }
            LOGGER.debug("Available archive logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$ARCHIVED_LOG");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain archive log table entries", e);
            }
            // LOGGER.debug("Available logs are:");
            // try {
            // logQueryResults(connection, "SELECT * FROM V$LOG");
            // }
            // catch (SQLException e) {
            // LOGGER.debug("Failed to obtain log table entries", e);
            // }
            // LOGGER.debug("Log history last 24 hours:");
            // try {
            // logQueryResults(connection, "SELECT * FROM V$LOG_HISTORY WHERE FIRST_TIME >= SYSDATE - 1");
            // }
            // catch (SQLException e) {
            // LOGGER.debug("Failed to obtain log history", e);
            // }
            LOGGER.debug("Log entries registered with LogMiner are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_LOGS");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain registered logs with LogMiner", e);
            }
            LOGGER.debug("Log mining session parameters are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_PARAMETERS");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain log mining session parameters", e);
            }
        }
    }

    /**
     * Helper method that dumps the result set of an arbitrary SQL query to the connector's logs.
     *
     * @param connection the database connection
     * @param query      the query to execute
     * @throws SQLException thrown if an exception occurs performing a SQL operation
     */
    private static void logQueryResults(DamengConnection connection, String query) throws SQLException {
        connection.query(query, rs -> {
            int columns = rs.getMetaData().getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int index = 1; index <= columns; ++index) {
                columnNames.add(rs.getMetaData().getColumnName(index));
            }
            LOGGER.debug("{}", columnNames);
            while (rs.next()) {
                List<Object> columnValues = new ArrayList<>();
                for (int index = 1; index <= columns; ++index) {
                    columnValues.add(rs.getObject(index));
                }
                LOGGER.debug("{}", columnValues);
            }
        });
    }

    /**
     * This method returns all archived log files for one day, containing given offset scn
     *
     * @param connection          connection
     * @param offsetScn           offset scn
     * @param archiveLogRetention duration that archive logs will be mined
     * @return list of archive logs
     * @throws SQLException if something happens
     */
    public static List<LogFile> getArchivedLogFilesForOffsetScn(DamengConnection connection, Scn offsetScn, Duration archiveLogRetention) throws SQLException {
        final List<LogFile> archiveLogFiles = new ArrayList<>();
        // try (PreparedStatement s = connection.connection(false).prepareStatement(LogMinerSqls.archiveLogsQuery(offsetScn, archiveLogRetention))) {
        try (PreparedStatement s = connection.connection(false).prepareStatement("select * from v$logmnr_logs")) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    Scn firstChangeNumber = Scn.valueOf(rs.getString(3));
                    Scn nextChangeNumber = rs.getString(2) == null ? Scn.MAX : Scn.valueOf(rs.getString(2));
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Archive log {} with SCN range {} to {} to be added.", fileName, firstChangeNumber, nextChangeNumber);
                    }
                    archiveLogFiles.add(new LogFile(fileName, firstChangeNumber, nextChangeNumber));
                }
            }
        }
        return archiveLogFiles;
    }

    /**
     * This method removes all added log files from mining
     *
     * @param conn connection
     * @throws SQLException something happened
     */
    public static void removeLogFilesFromMining(DamengConnector connector) throws SQLException {
        try (PreparedStatement ps = connector.connect().prepareStatement(LogMinerSqls.FILES_FOR_MINING);
             ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                executeCallableStatement(connector, LogMinerSqls.deleteLogFileStatement(fileName));
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        }
    }

    private static void executeCallableStatement(DamengConnector connector, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connector.connect().prepareCall(statement)) {
            s.execute();
        }
    }

    public static Map<String, String> getMap(DamengConnector connector, String query, String nullReplacement) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        try (
                PreparedStatement statement = connector.connect().prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String value = rs.getString(2);
                value = value == null ? nullReplacement : value;
                result.put(rs.getString(1), value);
            }
            return result;
        }
    }

    public static Object getSingleResult(DamengConnector connector, String query, DATATYPE type) throws SQLException {
        try (PreparedStatement statement = connector.connect().prepareStatement(query);
             ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                switch (type) {
                    case LONG:
                        return rs.getLong(1);
                    case TIMESTAMP:
                        return rs.getTimestamp(1);
                    case STRING:
                        return rs.getString(1);
                    case FLOAT:
                        return rs.getFloat(1);
                }
            }
            return null;
        }
    }
}
