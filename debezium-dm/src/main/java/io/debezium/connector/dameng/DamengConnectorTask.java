/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class DamengConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private volatile DamengTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile DamengConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile DamengDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        DamengConnectorConfig connectorConfig = new DamengConnectorConfig(config);
        TopicSelector<TableId> topicSelector = DamengTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        Configuration jdbcConfig = connectorConfig.jdbcConfig();
        jdbcConnection = new DamengConnection(jdbcConfig, () -> getClass().getClassLoader());
        this.schema = new DamengDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);
        this.schema.initializeStorage();

        String adapterString = config.getString(DamengConnectorConfig.CONNECTOR_ADAPTER);
        DamengConnectorConfig.ConnectorAdapter adapter = DamengConnectorConfig.ConnectorAdapter.parse(adapterString);
        OffsetContext previousOffset = getPreviousOffset(new DamengOffsetContext.Loader(connectorConfig, adapter));

        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        taskContext = new DamengTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new DamengErrorHandler(connectorConfig.getLogicalName(), queue);

        final DamengEventMetadataProvider metadataProvider = new DamengEventMetadataProvider();

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        final DamengStreamingChangeEventSourceMetrics streamingMetrics = new DamengStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig);

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                DamengConnector.class,
                connectorConfig,
                new DamengChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext, streamingMetrics),
                new DamengChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return DamengConnectorConfig.ALL_FIELDS;
    }
}
