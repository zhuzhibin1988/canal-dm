package com.eshore.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

public abstract class AbstractRedoLogParser<T> extends AbstractCanalLifeCycle implements RedoLogParser<T> {

    public void reset() {
    }

    public Entry parse(T event, TableMeta tableMeta) throws CanalParseException {
        return null;
    }

    public Entry parse(T event) throws CanalParseException {
        return null;
    }

    public void stop() {
        reset();
        super.stop();
    }

}
