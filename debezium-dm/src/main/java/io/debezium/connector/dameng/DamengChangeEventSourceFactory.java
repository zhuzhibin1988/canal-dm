/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class DamengChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final DamengConnectorConfig configuration;
    private final DamengConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final DamengDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final DamengTaskContext taskContext;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;

    public DamengChangeEventSourceFactory(DamengConnectorConfig configuration, DamengConnection jdbcConnection,
                                          ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, DamengDatabaseSchema schema,
                                          Configuration jdbcConfig, DamengTaskContext taskContext,
                                          DamengStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new DamengSnapshotChangeEventSource(configuration, (DamengOffsetContext) offsetContext, jdbcConnection,
                schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        DamengConnectorConfig.ConnectorAdapter adapter = configuration.getAdapter();
        return new LogMinerStreamingChangeEventSource(
                configuration,
                (DamengOffsetContext) offsetContext,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }
}
