package com.eshore.otter.canal.parse.inbound.dameng.dbsync;

import com.eshore.dbsync.logmnr.LogFetcher;
import com.eshore.otter.canal.parse.driver.dameng.RedoLog;
import com.eshore.otter.canal.parse.driver.dameng.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity) {
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity, final float growthFactor) {
        super(initialCapacity, growthFactor);
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
            if (!fetch0(0, NET_HEADER_SIZE)) {
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }
            position = origin;
            limit -= origin;
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

    private final boolean fetch0(Scn startScn, Scn endScn) throws SQLException {
        ensureCapacity(off + len);
        LogFile logFile = queryRedoLogFile();
        addLogFile(logFile);
        startLogmnr(startScn, endScn);
        ResultSet rs = queryRedoLog(startScn, endScn);
        while (rs.next()) {
            RedoLog redoLog = new RedoLog();
            this.buffer.add(redoLog);
        }
        removeLogFile(logFile);
        endLogmnr();
        return true;
    }

    private void initArchiveFileForMining() {

    }

    private LogFile queryRedoLogFile() throws SQLException {
        String sql = SqlUtils.FILES_FOR_MINING;
        PreparedStatement ps = this.connector.connect().prepareStatement(sql);
        return ps.executeQuery();
    }

    private List<RedoLog> queryRedoLog(Scn startScn, Scn endScn) throws SQLException {
        String sql = SqlUtils.logMinerContentsQuery("");
        PreparedStatement mineView = this.connector.connect().prepareStatement(sql);
        mineView.setString(1, startScn.toString());
        mineView.setString(2, endScn.toString());
        return mineView.executeQuery();
    }

    private void addLogFile(LogFile logFile) throws SQLException {
        String sql = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", logFile.getFileName());
        connector.connect().createStatement().execute(sql);
    }

    private void startLogmnr(Scn startScn, Scn endScn) throws SQLException {
        String sql = SqlUtils.startLogMinerStatement(startScn, endScn);
        connector.connect().createStatement().execute(sql);
    }

    private void endLogmnr() throws SQLException {
        this.connector.connect().createStatement().execute(SqlUtils.END_LOGMNR);
    }

    private void removeLogFile(LogFile logFile) throws SQLException {
        String sql = SqlUtils.deleteLogFileStatement(logFile.getFileName());
        this.connector.connect().createStatement().execute(sql);
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
