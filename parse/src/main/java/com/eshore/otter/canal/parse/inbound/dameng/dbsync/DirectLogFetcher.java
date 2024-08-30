package com.eshore.otter.canal.parse.inbound.dameng.dbsync;

import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;

import com.eshore.dbsync.logmnr.LogFetcher;
import com.eshore.otter.canal.parse.driver.dameng.DamengConnector;
import com.eshore.otter.canal.parse.driver.dameng.RedoLog;
import com.eshore.otter.canal.parse.driver.dameng.Scn;
import com.eshore.otter.canal.parse.driver.dameng.SqlUtils;
import com.eshore.otter.dbsync.logmnr.LogFetcher;
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

    public void start(DamengConnector connector) throws IOException {
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

            // Fetching the first packet(may a multi-packet).
            int netlen = getUint24(PACKET_LEN_OFFSET);
            int netnum = getUint8(PACKET_SEQ_OFFSET);
            if (!fetch0(NET_HEADER_SIZE, netlen)) {
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }

            // Detecting error code.
            final int mark = getUint8(NET_HEADER_SIZE);
            if (mark != 0) {
                if (mark == 255) // error from master
                {
                    // Indicates an error, for example trying to fetch from
                    // wrong
                    // binlog position.
                    position = NET_HEADER_SIZE + 1;
                    final int errno = getInt16();
                    String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
                    String errmsg = getFixString(limit - position);
                    throw new IOException("Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                            + " errmsg = " + errmsg);
                } else if (mark == 254) {
                    // Indicates end of stream. It's not clear when this would
                    // be sent.
                    logger.warn("Received EOF packet from server, apparent"
                            + " master disconnected. It's may be duplicate slaveId , check instance config");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                            + ", len = " + netlen);
                }
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

        String sql = SqlUtils.logMinerContentsQuery("");
        PreparedStatement mineView = this.connector.connection().prepareStatement(sql);
        mineView.setString(1, startScn.toString());
        mineView.setString(2, endScn.toString());
        ResultSet rs = mineView.executeQuery();
        while (rs.next()) {
            RedoLog redoLog = new RedoLog();
            this.buffer.add(redoLog);
        }
        return true;
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
