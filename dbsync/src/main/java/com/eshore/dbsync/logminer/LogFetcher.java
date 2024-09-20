package com.eshore.dbsync.logminer;

import com.eshore.dbsync.logminer.event.dameng.RedoLog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Declaration a binary-log fetcher. It extends from <code>LogBuffer</code>.
 *
 * <pre>
 * LogFetcher fetcher = new SomeLogFetcher();
 * ...
 *
 * while (fetcher.fetch())
 * {
 *     LogEvent event;
 *     do
 *     {
 *         event = decoder.decode(fetcher, context);
 *
 *         // process log event.
 *     }
 *     while (event != null);
 * }
 * // no more binlog.
 * fetcher.close();
 * </pre>
 *
 * @author zhuzhibin
 * @version 1.0
 */
public abstract class LogFetcher implements Closeable {

    /**
     * Default initial capacity.
     */
    public static final int DEFAULT_INITIAL_CAPACITY = 1000;


    protected final List<RedoLog> buffer;

    public LogFetcher() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public LogFetcher(final int initialCapacity) {
        this.buffer = new ArrayList(initialCapacity);
    }

    /**
     * Fetches the next frame of binary-log, and fill it in buffer.
     */
    public abstract boolean fetch() throws IOException;

    /**
     * {@inheritDoc}
     *
     * @see Closeable#close()
     */
    public abstract void close() throws IOException;
}
