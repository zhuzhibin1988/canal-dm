/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.otter.canal.parse.driver.dameng;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <pre>
 *  <ul>达梦jdbc连接器
 *      <li>classloader加载jdbc驱动类</li>
 *      <li>创建connection，并测试连接状态</li>
 *      <li>执行sql查询</li>
 *  </ul>
 * </pre>
 */

public class DamengConnector implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnector.class);


    /**
     * Pattern to identify system generated indices and column names.
     */
    private static final Pattern SYS_NC_PATTERN = Pattern.compile("^SYS_NC(?:_OID|_ROWINFO|[0-9][0-9][0-9][0-9][0-9])\\$$");


    private static final String JDBC_PATTERN = "jdbc:dm//${hostname}:${port}";

    private static final String JDBC_DRIVER = "dm.jdbc.driver.DmDriver";

    /**
     * The database version.
     */
    private final DamengDatabaseVersion databaseVersion;

    private Properties prop;
    private ConnectionFactory factory;
    private Connection conn;


    public DamengConnector(InetSocketAddress socketAddress,
                           String username,
                           String password) {
        this.prop = new Properties();
        this.prop.put(DamengJdbcConfiguration.HOSTNAME, socketAddress.getHostName());
        this.prop.put(DamengJdbcConfiguration.PORT, socketAddress.getPort());
        this.prop.put(DamengJdbcConfiguration.USERNAME, username);
        this.prop.put(DamengJdbcConfiguration.PASSWORD, password);
        this.factory = prepareConnectionFactory();

        this.databaseVersion = resolveDmDatabaseVersion();
        LOGGER.info("Database Version: {}", databaseVersion.getBanner());
    }


    @FunctionalInterface
    @ThreadSafe
    public interface ConnectionFactory {
        Connection getConnection(Properties prop) throws SQLException;
    }

    private synchronized ConnectionFactory prepareConnectionFactory() {
        return (prop) -> {
            ClassLoader classLoader = DamengConnector.class.getClassLoader();
            Connection conn = null;
            try {
                Class<Driver> driverClass = (Class<Driver>) Class.forName(JDBC_DRIVER, true, classLoader);
                Driver driver = driverClass.getDeclaredConstructor().newInstance();
                conn = driver.connect(findAndReplace(JDBC_PATTERN, prop), prop);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("dameng driver is not found, check classpath include driver lib or not");
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                e.printStackTrace();
            }
            return conn;
        };
    }

    private String findAndReplace(String urlPattern, Properties prop) {
        String url = urlPattern;
        for (Map.Entry kv : prop.entrySet()) {
            String key = kv.getKey().toString();
            String value = kv.getValue().toString();
            if (urlPattern.contains("${" + key + "}")) {
                url = url.replaceAll("\\$\\{" + key + "}", value);
            }
        }
        return url;
    }

    public synchronized Connection connection() throws SQLException {
        if (isConnected()) {
            return this.conn;
        } else {
            this.conn = this.factory.getConnection(this.prop);
        }
        return this.conn;
    }

    public synchronized boolean isConnected() throws SQLException {
        if (this.conn == null) {
            return false;
        }
        return !this.conn.isClosed();
    }

    public synchronized void disConnection() throws SQLException {
        if (this.conn != null) {
            this.conn.close();
        }
    }

    private DamengDatabaseVersion resolveDmDatabaseVersion() {
        String versionStr = null;
        try {
            // Oracle 18.1 introduced BANNER_FULL as the new column rather than BANNER
            // This column uses a different format than the legacy BANNER.
            ResultSet rs = connection().createStatement().executeQuery("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'DM Database%'");
            if (rs.next()) {
                versionStr = rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to resolve dm database version", e);
        }

        if (versionStr == null) {
            throw new RuntimeException("Failed to resolve dm database version");
        }

        return DamengDatabaseVersion.parse(versionStr);
    }


    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern, String[] tableTypes) throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream().map(t -> new TableId(databaseCatalog, t.schema(), t.table())).collect(Collectors.toSet());
    }

    /**
     * Retrieves all {@code TableId} in a given database catalog, filtering certain ids that should be
     * omitted from the returned set such as special spatial tables and index-organized tables.
     *
     * @param catalogName the catalog/database name
     * @return set of all table ids for existing table objects
     * @throws SQLException if a database exception occurred
     */
    protected Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = "select owner, table_name from all_tables " +
                // filter special spatial tables
                "where table_name NOT LIKE 'MDRT_%' " + "and table_name NOT LIKE 'MDRS_%' " + "and table_name NOT LIKE 'MDXT_%' " +
                // filter index-organized-tables
                "and (table_name NOT LIKE 'SYS_IOT_OVER_%' " +
                // "and IOT_NAME IS NULL" +
                ") ";

        Set<TableId> tableIds = new HashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(new TableId(catalogName, rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    // todo replace metadata with something like this
    private ResultSet getTableColumnsInfo(String schemaNamePattern, String tableName) throws SQLException {
        String columnQuery = "select column_name, data_type, data_length, data_precision, data_scale, default_length, density, char_length from all_tab_columns where owner like '"
                + schemaNamePattern + "' and table_name='" + tableName + "'";

        PreparedStatement statement = connection().prepareStatement(columnQuery);
        return statement.executeQuery();
    }

    // this is much faster, we will use it until full replacement of the metadata usage TODO
    public void readSchemaForCapturedTables(Tables tables, String databaseCatalog, String schemaNamePattern, ColumnNameFilter columnFilter,
                                            boolean removeTablesNotFoundInJdbc, Set<TableId> capturedTables) throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                        columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>()).add(column.create());
                    });
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }

        for (TableId tableId : capturedTables) {
            overrideOracleSpecificColumnTypes(tables, tableId, tableId);
        }
    }

    @Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableFilter tableFilter, ColumnNameFilter columnFilter,
                           boolean removeTablesNotFoundInJdbc) throws SQLException {

        super.readSchema(tables, null, schemaNamePattern, tableFilter, columnFilter, removeTablesNotFoundInJdbc);

        Set<TableId> tableIds = tables.tableIds().stream().filter(x -> schemaNamePattern.equals(x.schema())).collect(Collectors.toSet());

        for (TableId tableId : tableIds) {
            // super.readSchema() populates ids without the catalog; hence we apply the filtering only
            // here and if a table is included, overwrite it with a new id including the catalog
            TableId tableIdWithCatalog = new TableId(databaseCatalog, tableId.schema(), tableId.table());

            if (tableFilter.isIncluded(tableIdWithCatalog)) {
                overrideOracleSpecificColumnTypes(tables, tableId, tableIdWithCatalog);
            }

            tables.removeTable(tableId);
        }
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter) throws SQLException {
        // Oracle drivers require this for LONG/LONGRAW to be fetched first.
        final String defaultValue = columnMetadata.getString(13);

        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }
            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented(false);
            String autogenerated = null;
            autogenerated = "NO";
            column.generated("YES".equalsIgnoreCase(autogenerated));

            column.nativeType(resolveNativeType(column.typeName()));
            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));
            if (defaultValue != null) {
                getDefaultValue(column.create(), defaultValue).ifPresent(column::defaultValue);
            }
            return Optional.of(column);
        }

        return Optional.empty();
    }

    public DamengConnector executeLegacy(Operations operations) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement()) {
            operations.apply(statement);
            commit();
        }
        return this;
    }

    @Override
    public void close() throws Exception {
        disConnection();
    }

}
