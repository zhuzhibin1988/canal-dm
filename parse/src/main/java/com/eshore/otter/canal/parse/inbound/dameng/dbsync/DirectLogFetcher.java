package com.eshore.otter.canal.parse.inbound.dameng.dbsync;

import com.eshore.dbsync.logminer.LogFetcher;
import com.eshore.dbsync.logminer.event.dameng.LogFile;
import com.eshore.dbsync.logminer.event.dameng.RedoLog;
import com.eshore.dbsync.logminer.event.dameng.Scn;
import com.eshore.otter.canal.parse.driver.dameng.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 基于socket的logEvent实现
 *
 * @author jianghang 2013-1-14 下午07:39:30
 * @version 1.0.0
 */
public class DirectLogFetcher extends LogFetcher {

    protected static final Logger logger = LoggerFactory.getLogger(DirectLogFetcher.class);

    // Master heartbeat interval
    public static final int MASTER_HEARTBEAT_PERIOD_SECONDS = 15;
    // +10s 确保 timeout > heartbeat interval
    private static final int READ_TIMEOUT_MILLISECONDS = (MASTER_HEARTBEAT_PERIOD_SECONDS + 10) * 1000;
    /**
     * Maximum packet length
     */
    public static final int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);

    private DamengConnector connector;

    // private BufferedInputStream input;

    public DirectLogFetcher() {
        super(DEFAULT_INITIAL_CAPACITY);
    }

    public DirectLogFetcher(final int initialCapacity) {
        super(initialCapacity);
    }

    public void start(DamengConnector connector) throws SQLException {
        this.connector = connector;
    }

    /**
     * {@inheritDoc}
     *
     * @see LogFetcher#fetch()
     */
    public boolean fetch() throws IOException {
        try {
            // Fetching packet header from input.
            if (!doFetch(0, NET_HEADER_SIZE)) {
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }
            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException | ClosedByInterruptException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    private final boolean doFetch(Scn startScn, Scn endScn) throws SQLException {
        LogFile logFile = queryRedoLogFile();
        addLogFile(logFile);
        startLogmnr(startScn, endScn);
        List<RedoLog> redoLogs = queryRedoLogs(startScn, endScn);
        this.buffer.clear();
        this.buffer.addAll(redoLogs);
        removeLogFile(logFile);
        endLogmnr();
        return true;
    }

    private void initializeRedoLogsForMining(boolean postEndMiningSession, Duration archiveLogRetention) throws SQLException {
        if (!postEndMiningSession) {
            if (DamengConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                // buildDataDictionary(connection);
            }
            if (!isContinuousMining) {
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        } else {
            if (!isContinuousMining) {
                if (DamengConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    // buildDataDictionary(connection);
                }
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        }
    }

    private LogFile queryRedoLogFile() throws SQLException {
        String sql = LogMinerSqls.FILES_FOR_MINING;
        DamengQueryExecutor damengQueryExecutor = new DamengQueryExecutor(this.connector);
        List<LogFile> logFiles = damengQueryExecutor.query(sql, new DamengQueryExecutor.ResultSetProcessor<LogFile>() {
            @Override
            public List<LogFile> process(ResultSet rs) {
                List<LogFile> logFiles = new ArrayList<>();
                try {
                    while (rs.next()) {
                        LogFile logFile = new LogFile(rs.getString("filename"),
                                Scn.valueOf(rs.getString("low_scn")),
                                Scn.valueOf(rs.getString("next_scn")),
                                true
                        );
                    }
                } catch (SQLException e) {

                }
                return logFiles;
            }
        });
        return logFiles.get(0);
    }

    private List<RedoLog> queryRedoLogs(Scn startScn, Scn endScn) throws SQLException {
        String sql = String.format(LogMinerSqls.logMinerContentsQuery(""), startScn.toString(), endScn.toString());
        DamengQueryExecutor damengQueryExecutor = new DamengQueryExecutor(this.connector);
        List<RedoLog> redoLogs = damengQueryExecutor.query(sql, new DamengQueryExecutor.ResultSetProcessor() {
            @Override
            public List process(ResultSet rs) {
                return null;
            }
        });
        return redoLogs;
    }

    private void addLogFile(LogFile logFile) throws SQLException {
        String sql = LogMinerSqls.addLogFileStatement("DBMS_LOGMNR.ADDFILE", logFile.getFileName());
        DamengQueryExecutor damengQueryExecutor = new DamengQueryExecutor(this.connector);
        damengQueryExecutor.execute(sql);
    }

    private void startLogmnr(Scn startScn, Scn endScn) throws SQLException {
        String sql = LogMinerSqls.startLogMinerStatement(startScn, endScn);
        DamengQueryExecutor damengQueryExecutor = new DamengQueryExecutor(this.connector);
        damengQueryExecutor.execute(sql);
    }

    private void endLogmnr() throws SQLException {
        DamengQueryExecutor damengQueryExecutor = new DamengQueryExecutor(this.connector);
        damengQueryExecutor.execute(LogMinerSqls.END_LOGMNR);
    }

    private void removeLogFile(LogFile logFile) throws SQLException {
        String sql = LogMinerSqls.deleteLogFileStatement(logFile.getFileName());
        DamengQueryExecutor damengQueryExecutor = new DamengQueryExecutor(this.connector);
        damengQueryExecutor.execute(sql);
    }

    public void setRedoLogFilesForMining(Scn lastProcessedScn, Duration archiveLogRetention) throws SQLException {

        removeLogFilesFromMining();

        List<LogFile> onlineLogFilesForMining = getOnlineLogFilesForOffsetScn(lastProcessedScn);
        List<LogFile> archivedLogFilesForMining = getArchivedLogFilesForOffsetScn(lastProcessedScn, archiveLogRetention);

        final boolean hasOverlappingLogFile = onlineLogFilesForMining.stream()
                .anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0)
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
            logger.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = LogMinerSqls.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(addLogFileStatement);
        }

        logger.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    public List<LogFile> getOnlineLogFilesForOffsetScn(Scn offsetScn) throws SQLException {
        logger.trace("Getting online redo logs for offset scn {}", offsetScn);
        List<LogFile> redoLogFiles = new ArrayList<>();

        // try (PreparedStatement s = connection.connection(false).prepareStatement(LogMinerSqls.allOnlineLogsQuery())) {
        String sql = "SELECT PATH,ARCH_LSN,CLSN,STATUS FROM SYS.V$ARCH_FILE ";
        try (PreparedStatement s = this.connector.connect().prepareStatement(sql)) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    Scn firstChangeNumber = getScnFromString(rs.getString(2));
                    Scn nextChangeNumber = getScnFromString(rs.getString(3));
                    String status = rs.getString(4);
                    LogFile logFile = new LogFile(fileName, firstChangeNumber, nextChangeNumber, "ACTIVE".equalsIgnoreCase(status));
                    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        logger.trace("Online redo log {} with SCN range {} to {} ({}) to be added.", fileName, firstChangeNumber, nextChangeNumber, status);
                        redoLogFiles.add(logFile);
                    } else {
                        logger.trace("Online redo log {} with SCN range {} to {} ({}) to be excluded.", fileName, firstChangeNumber, nextChangeNumber, status);
                    }
                }
            }
        }
        return redoLogFiles;
    }

    /**
     * This method returns all archived log files for one day, containing given offset scn
     *
     * @param offsetScn           offset scn
     * @param archiveLogRetention duration that archive logs will be mined
     * @return list of archive logs
     * @throws SQLException if something happens
     */
    private List<LogFile> getArchivedLogFilesForOffsetScn(Scn offsetScn, Duration archiveLogRetention) throws SQLException {
        final List<LogFile> archiveLogFiles = new ArrayList<>();
        // try (PreparedStatement s = connection.connection(false).prepareStatement(LogMinerSqls.archiveLogsQuery(offsetScn, archiveLogRetention))) {
        try (PreparedStatement s = this.connector.connect().prepareStatement(LogMinerSqls.FILES_FOR_MINING)) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    Scn firstChangeNumber = Scn.valueOf(rs.getString(3));
                    Scn nextChangeNumber = rs.getString(2) == null ? Scn.MAX : Scn.valueOf(rs.getString(2));
                    archiveLogFiles.add(new LogFile(fileName, firstChangeNumber, nextChangeNumber));
                }
            }
        }
        return archiveLogFiles;
    }

    /**
     * This method removes all added log files from mining
     *
     * @throws SQLException something happened
     */
    private void removeLogFilesFromMining() throws SQLException {
        try (PreparedStatement ps = this.connector.connect().prepareStatement(LogMinerSqls.FILES_FOR_MINING);
             ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                executeCallableStatement(LogMinerSqls.deleteLogFileStatement(fileName));
                logger.debug("File {} was removed from mining", fileName);
            }
        }
    }

    private void executeCallableStatement(String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = this.connector.connect().prepareCall(statement)) {
            s.execute();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see LogFetcher#close()
     */
    public void close() throws IOException {
        // do nothing
    }
}
