package com.eshore.dbsync.logminer.event.dameng;


import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

import java.time.Instant;
import java.util.Map;

public class ScnContext {

    private final SourceInfo sourceInfo;



    public ScnContext(Scn scn, Scn commitScn) {
        this(scn);
        sourceInfo.setCommitScn(commitScn);
    }

    private ScnContext(Scn scn) {
        sourceInfo = new SourceInfo();
        sourceInfo.setScn(scn);
    }

    public static class Builder {
        private Scn scn;

        public Builder logicalName() {
            return this;
        }

        public Builder scn(Scn scn) {
            this.scn = scn;
            return this;
        }


        ScnContext build() {
            return new ScnContext(scn);
        }
    }

    public static Builder create() {
        return new Builder();
    }


    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public void setScn(Scn scn) {
        sourceInfo.setScn(scn);
    }

    public void setCommitScn(Scn commitScn) {
        sourceInfo.setCommitScn(commitScn);
    }

    public Scn getScn() {
        return sourceInfo.getScn();
    }

    public Scn getCommitScn() {
        return sourceInfo.getCommitScn();
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setTableId(TableId tableId) {
        sourceInfo.setTableId(tableId);
    }


    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setTableId((TableId) tableId);
        sourceInfo.setSourceTime(timestamp);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }


    public static Scn getScnFromOffset(Map<String, ?> offset) {
        // Prioritize string-based SCN key over the numeric-based SCN key
        Object scn = offset.get(SourceInfo.SCN_KEY);
        if (scn instanceof String) {
            return Scn.valueOf((String) scn);
        } else if (scn != null) {
            return Scn.valueOf((Long) scn);
        }
        return null;
    }


    /**
     * Helper method to resolve a {@link Scn} by key from the offset map.
     *
     * @param offset the offset map
     * @param key    the entry key, either {@link SourceInfo#SCN_KEY} or {@link SourceInfo#COMMIT_SCN_KEY}.
     * @return the {@link Scn} or null if not found
     */
    public static Scn getScnFromOffsetMapByKey(Map<String, ?> offset, String key) {
        Object scn = offset.get(key);
        if (scn instanceof String) {
            return Scn.valueOf((String) scn);
        } else if (scn != null) {
            return Scn.valueOf((Long) scn);
        }
        return null;
    }
}
