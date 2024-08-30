/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.otter.canal.parse.driver.dameng;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Instant;

@NotThreadSafe
public class SourceInfo {

    public static final String TXID_KEY = "txId";
    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";
    public static final String SNAPSHOT_KEY = "snapshot";

    private Scn scn;
    private Scn commitScn;
    private String transactionId;
    private Instant sourceTime;
    private TableId tableId;

    protected SourceInfo(DamengConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public Scn getScn() {
        return scn;
    }

    public Scn getCommitScn() {
        return commitScn;
    }

    public void setScn(Scn scn) {
        this.scn = scn;
    }

    public void setCommitScn(Scn commitScn) {
        this.commitScn = commitScn;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    protected Instant timestamp() {
        return sourceTime;
    }

    protected String database() {
        return tableId.catalog();
    }
}
