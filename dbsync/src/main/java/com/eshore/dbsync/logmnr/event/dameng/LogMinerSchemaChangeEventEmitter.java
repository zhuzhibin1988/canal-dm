/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.dbsync.logmnr.event.dameng;

import io.debezium.connector.dameng.BaseOracleSchemaChangeEventEmitter;
import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.connector.dameng.logminer.valueholder.LogMinerDdlEntry;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle LogMiner utility.
 */
public class LogMinerSchemaChangeEventEmitter extends BaseOracleSchemaChangeEventEmitter {

    public LogMinerSchemaChangeEventEmitter(DamengOffsetContext offsetContext, TableId tableId, LogMinerDdlEntry ddlLcr) {
        super(offsetContext,
                tableId,
                tableId.catalog(), // todo tableId should be enough
                tableId.schema(), // todo same here
                ddlLcr.getDdlText(),
                ddlLcr.getCommandType());
    }
}
